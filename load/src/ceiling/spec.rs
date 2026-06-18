use std::thread::available_parallelism;

#[derive(Debug, Clone)]
pub struct BoxSpec {
    pub logical_cores: usize,
    pub physical_cores: usize,
    pub total_ram_bytes: u64,
    pub cgroup_mem_limit: Option<u64>,
    pub kernel: String,
}

pub fn parse_mem_total_kb(meminfo: &str) -> Option<u64> {
    meminfo.lines().find_map(|l| {
        l.strip_prefix("MemTotal:")?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
}

pub fn parse_physical_cores(cpuinfo: &str) -> Option<usize> {
    // A physical core is unique by (physical id, core id) — `core id` alone is only
    // unique within a socket, so counting it would undercount on multi-socket machines.
    // Blocks are delimited by `processor` lines; `physical id` defaults to "0" (single
    // socket) when the kernel omits it.
    let val = |l: &str, key: &str| {
        l.strip_prefix(key)
            .map(|r| r.trim_start_matches([':', ' ', '\t']).trim().to_string())
    };
    let mut pairs: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    let mut phys = String::from("0");
    let mut core: Option<String> = None;
    let mut in_block = false;
    for line in cpuinfo.lines() {
        if line.starts_with("processor") {
            if in_block {
                if let Some(c) = core.take() {
                    pairs.insert((phys.clone(), c));
                }
            }
            phys = String::from("0");
            core = None;
            in_block = true;
        } else if let Some(v) = val(line, "physical id") {
            phys = v;
        } else if let Some(v) = val(line, "core id") {
            core = Some(v);
        }
    }
    if in_block {
        if let Some(c) = core.take() {
            pairs.insert((phys, c));
        }
    }
    if pairs.is_empty() {
        None
    } else {
        Some(pairs.len())
    }
}

pub fn parse_cgroup_max(s: &str) -> Option<u64> {
    let t = s.trim();
    if t == "max" {
        None
    } else {
        t.parse().ok()
    }
}

pub fn detect() -> BoxSpec {
    let logical_cores = available_parallelism().map(|n| n.get()).unwrap_or(1);
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let physical_cores = parse_physical_cores(&cpuinfo).unwrap_or(logical_cores);
    let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let total_ram_bytes = parse_mem_total_kb(&meminfo).unwrap_or(0) * 1024;
    let cgroup_mem_limit = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
        .ok()
        .and_then(|s| parse_cgroup_max(&s));
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .unwrap_or_default()
        .trim()
        .to_string();
    BoxSpec {
        logical_cores,
        physical_cores,
        total_ram_bytes,
        cgroup_mem_limit,
        kernel,
    }
}

pub fn effective_mem_bytes(s: &BoxSpec) -> u64 {
    match s.cgroup_mem_limit {
        Some(c) if c < s.total_ram_bytes => c,
        _ => s.total_ram_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mem_total_parses() {
        let s = "MemTotal:       98566160 kB\nMemFree: 1234 kB\n";
        assert_eq!(parse_mem_total_kb(s), Some(98_566_160));
    }

    #[test]
    fn physical_cores_counts_unique_core_ids() {
        // two logical CPUs sharing core id 0 (HT), one on core id 1 → 2 physical
        let s = "processor\t: 0\ncore id\t\t: 0\nprocessor\t: 1\ncore id\t\t: 0\nprocessor\t: 2\ncore id\t\t: 1\n";
        assert_eq!(parse_physical_cores(s), Some(2));
    }

    #[test]
    fn physical_cores_counts_across_sockets() {
        // 2 sockets, each with core id 0 → 2 distinct physical cores, NOT 1
        // (the old core-id-only counter would have returned 1 here).
        let s = "processor\t: 0\nphysical id\t: 0\ncore id\t\t: 0\n\n\
                 processor\t: 1\nphysical id\t: 1\ncore id\t\t: 0\n";
        assert_eq!(parse_physical_cores(s), Some(2));
    }

    #[test]
    fn cgroup_max_parses_number_and_max() {
        assert_eq!(parse_cgroup_max("268435456\n"), Some(268_435_456));
        assert_eq!(parse_cgroup_max("max\n"), None);
    }
}
