use std::arch::asm;

#[inline]
pub fn cpuid() {
    unsafe {
        let mut _eax: u32 = 0x2;
        let mut _ecx: u32 = 0x0;
        let mut _edx: u32;
        asm!(
            "cpuid",
            inout("eax") _eax,
            inout("ecx") _ecx,
            lateout("edx") _edx,
        );
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
