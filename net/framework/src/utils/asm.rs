use std::arch::asm;

#[inline]
pub fn cpuid() {
    unsafe {
        asm!("mov eax, 0x2", out("eax") _);
        asm!("mov ecx, 0x0", out("ecx") _);
        asm!("cpuid", out("eax") _, out("ebx") _, out("ecx") _, out("edx") _);
    }
}

#[inline]
pub fn rdtsc_unsafe() -> u64 {
    unsafe {
        let low: u32;
        let high: u32;
        asm!("rdtsc", out("eax") low, out("edx") high);
        ((high as u64) << 32) | (low as u64)
    }
}

#[inline]
pub fn rdtscp_unsafe() -> u64 {
    let high: u32;
    let low: u32;
    unsafe {
        asm!("rdtscp", out("eax") low, out("edx") high, out("ecx") _);
        ((high as u64) << 32) | (low as u64)
    }
}

#[inline]
pub fn pause() {
    unsafe {
        asm!("pause");
    }
}
