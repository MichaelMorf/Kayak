use core::arch::asm;

#[inline]
pub fn cpuid() {
    unsafe {
        asm!(
            "mov eax, 0x2",
            "mov ecx, 0x0",
            "cpuid",
            out("eax") _, out("esi") _, out("ecx") _, out("edx") _,
            options(nomem, nostack, preserves_flags)
        );
    }
}

#[inline]
pub fn rdtsc_unsafe() -> u64 {
    unsafe {
        let mut low: u32;
        let mut high: u32;
        asm!(
            "rdtsc",
            out("eax") low,
            out("edx") high,
            options(nomem, nostack, preserves_flags)
        );
        ((high as u64) << 32) | (low as u64)
    }
}

#[inline]
pub fn rdtscp_unsafe() -> u64 {
    let mut high: u32;
    let mut low: u32;
    unsafe {
        asm!(
            "rdtscp",
            out("eax") low,
            out("edx") high,
            out("ecx") _,
            options(nomem, nostack, preserves_flags)
        );
        ((high as u64) << 32) | (low as u64)
    }
}

#[inline]
pub fn pause() {
    unsafe {
        asm!("pause", options(nomem, nostack, preserves_flags));
    }
}
