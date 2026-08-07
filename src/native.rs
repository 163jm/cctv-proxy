/// Native library bindings for delib (TS decryption) and media_utils (JSTV signing)
use anyhow::{anyhow, Result};
use libloading::{Library, Symbol};
use std::path::Path;
use tracing::{info, warn};

// ─── Platform helpers ────────────────────────────────────────────────────────

/// Return the platform-appropriate shared-library file name.
/// Windows: delib.dll / media_utils.dll
/// macOS:   delib.dylib / media_utils.dylib
/// Linux:   delib.so / media_utils.so
pub(crate) fn platform_lib_name(base: &str) -> String {
    #[cfg(target_os = "windows")]
    {
        format!("{}.dll", base)
    }
    #[cfg(target_os = "macos")]
    {
        format!("{}.dylib", base)
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        format!("{}.so", base)
    }
}

/// Warn loudly when a file is a Linux ELF binary but we're running on Windows.
/// The original project ships Linux (often ARM64) chrome/ + native libs; they
/// cannot be executed or dlopen'd on Windows, which is the #1 reason every
/// browser/signer-dependent channel fails there.
pub(crate) fn warn_if_elf_on_windows(path: &std::path::Path, what: &str) {
    if !cfg!(windows) {
        return;
    }
    use std::io::Read;
    let Ok(mut f) = std::fs::File::open(path) else {
        return;
    };
    let mut magic = [0u8; 4];
    if f.read_exact(&mut magic).is_ok() && magic == [0x7f, b'E', b'L', b'F'] {
        warn!(
            "{} \"{}\" is a Linux ELF binary — it cannot be used on Windows. \
             Provide the Windows version (delib.dll / media_utils.dll / headless-shell.exe) \
             for these channels to work.",
            what,
            path.display(),
        );
    }
}

// ─── TS Decryptor (delib) ────────────────────────────────────────────────────

type DecryptTsFn = unsafe extern "C" fn(*const u8, usize) -> *mut u8;
type FreeDecryptedFn = unsafe extern "C" fn(*mut u8, usize);

pub struct TsDecryptor {
    // Keep library alive as long as the decryptor lives
    _lib: Library,
    decrypt_ts: DecryptTsFn,
    free_decrypted: FreeDecryptedFn,
}

impl TsDecryptor {
    pub fn load(lib_path: impl AsRef<Path>) -> Result<Self> {
        let path = lib_path.as_ref();
        if !path.exists() {
            return Err(anyhow!("delib not found at {:?}", path));
        }
        unsafe {
            let lib = Library::new(path)?;
            let decrypt_ts: Symbol<DecryptTsFn> = lib.get(b"decrypt_ts\0")?;
            let free_decrypted: Symbol<FreeDecryptedFn> = lib.get(b"free_decrypted\0")?;
            // Copy the function pointers out so we can move `lib`
            let decrypt_ts = *decrypt_ts;
            let free_decrypted = *free_decrypted;
            info!("Loaded delib (TS decryptor) from {}", path.display());
            Ok(Self {
                _lib: lib,
                decrypt_ts,
                free_decrypted,
            })
        }
    }

    /// Attempt to decrypt a TS segment. Returns None if decryption fails or
    /// the output is not a valid MPEG-TS stream (sync-byte check).
    pub fn decrypt(&self, data: &[u8]) -> Option<Vec<u8>> {
        if data.len() < 188 {
            return None;
        }
        unsafe {
            let ptr = (self.decrypt_ts)(data.as_ptr(), data.len());
            if ptr.is_null() {
                return None;
            }
            // The native lib returns a buffer of the same length
            let out = std::slice::from_raw_parts(ptr, data.len()).to_vec();
            (self.free_decrypted)(ptr, data.len());

            // Validate MPEG-TS sync bytes (0x47 every 188 bytes).
            // Mirror original JS: check up to 10 packets; if invalid, return None
            // so caller falls back to raw data.
            let check_count = (data.len() / 188).min(10);
            let valid = check_count == 0
                || (0..check_count).all(|i| out[i * 188] == 0x47);
            if valid { Some(out) } else { None }
        }
    }
}

// SAFETY: TsDecryptor's native calls are stateless transforms; safe to share across threads.
unsafe impl Send for TsDecryptor {}
unsafe impl Sync for TsDecryptor {}

// ─── JSTV URL Signer (media_utils) ──────────────────────────────────────────

// The native function signature: const char* get_signed_url(const char* channel_id)
type GetSignedUrlFn = unsafe extern "C" fn(*const std::ffi::c_char) -> *const std::ffi::c_char;
type FreeSignedUrlFn = unsafe extern "C" fn(*const std::ffi::c_char);

pub struct JstvSigner {
    _lib: Library,
    get_signed_url: GetSignedUrlFn,
    free_signed_url: Option<FreeSignedUrlFn>,
}

impl JstvSigner {
    pub fn load(lib_path: impl AsRef<Path>) -> Result<Self> {
        let path = lib_path.as_ref();
        if !path.exists() {
            return Err(anyhow!("media_utils not found at {:?}", path));
        }
        unsafe {
            let lib = Library::new(path)?;
            let get_signed_url: Symbol<GetSignedUrlFn> = lib.get(b"get_signed_url\0")?;
            let get_signed_url = *get_signed_url;

            // free_signed_url may or may not be exported
            let free_signed_url: Option<FreeSignedUrlFn> = lib
                .get::<FreeSignedUrlFn>(b"free_signed_url\0")
                .ok()
                .map(|s| *s);

            info!("Loaded media_utils (JSTV signer) from {}", path.display());
            Ok(Self {
                _lib: lib,
                get_signed_url,
                free_signed_url,
            })
        }
    }

    /// Returns a signed stream URL for the given JSTV channel ID, or None on failure.
    pub fn get_signed_url(&self, channel_id: &str) -> Option<String> {
        use std::ffi::{CStr, CString};
        let c_id = CString::new(channel_id).ok()?;
        unsafe {
            let ptr = (self.get_signed_url)(c_id.as_ptr());
            if ptr.is_null() {
                return None;
            }
            let s = CStr::from_ptr(ptr).to_string_lossy().into_owned();
            if let Some(free_fn) = self.free_signed_url {
                free_fn(ptr);
            }
            if s.is_empty() { None } else { Some(s) }
        }
    }
}

unsafe impl Send for JstvSigner {}
unsafe impl Sync for JstvSigner {}

// ─── Combined native services ────────────────────────────────────────────────

pub struct NativeLibs {
    /// delib: handles a private container format (\x00\x00\x01 header),
    /// NOT standard MPEG-TS. CDN segments are already plain TS (0x47 sync byte).
    /// Kept here for completeness but not called in the proxy path.
    pub decryptor: Option<TsDecryptor>,
    /// media_utils: ECDSA/RSA signing for JSTV channels (js_, zj_, sd_, sh_)
    pub signer: Option<JstvSigner>,
}

impl NativeLibs {
    /// Load the native libraries, searching `dirs` in order.
    ///
    /// `dirs` should list the executable's own directory first (new default
    /// layout: delib/media_utils sit next to the tv-proxy binary), followed by
    /// `{app_dir}/chrome` for backward compatibility with the old layout.
    pub fn load(dirs: &[&Path], jstv_auth_enabled: bool) -> Self {
        let decryptor = dirs.iter().find_map(|dir| {
            let lib_path = dir.join(platform_lib_name("delib"));
            warn_if_elf_on_windows(&lib_path, "TS decryptor");
            TsDecryptor::load(&lib_path)
                .map_err(|e| warn!("delib not loaded ({}): {}", lib_path.display(), e))
                .ok()
        });

        let signer = if jstv_auth_enabled {
            dirs.iter().find_map(|dir| {
                let lib_path = dir.join(platform_lib_name("media_utils"));
                warn_if_elf_on_windows(&lib_path, "JSTV signer");
                JstvSigner::load(&lib_path)
                    .map_err(|e| warn!("media_utils not loaded ({}): {}", lib_path.display(), e))
                    .ok()
            })
        } else {
            None
        };

        Self { decryptor, signer }
    }
}
