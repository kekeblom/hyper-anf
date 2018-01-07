extern crate num_bigint;

use std::cmp;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};
use num_bigint::BigUint;

pub struct HyperLogLog {
    p: u8,
    m: usize,
    registers: Arc<RwLock<Vec<u8>>>,
    alpha: f64
}

impl HyperLogLog {
    pub fn new(p: u8) -> HyperLogLog {
        let m: usize = 1 << p;
        let mut registers = Vec::with_capacity(m as usize);
        for _ in 0..m {
            registers.push(0);
        }
        HyperLogLog {
            p: p,
            m: m,
            registers: Arc::new(RwLock::new(registers)),
            alpha: Self::compute_alpha(p),
        }
    }

    pub fn add(&mut self, value: &BigUint) {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        let hash: u64 = hasher.finish();
        let index = hash as usize & (self.m - 1);
        let first_bit = self.first_set_bit(hash >> self.p);
        let value = &mut self.registers.write().unwrap()[index];
        if *value < first_bit {
            *value = first_bit;
        }
    }

    pub fn count(&self) -> f64 {
        let estimate = self.estimate();
        let m_f64 = self.m as f64;
        let two_32 = (1_i64 << 32) as f64;
        let corrected_estimate;
        if estimate <= (5_f64 / 2_f64 * m_f64) {
            let zeros = self.zero_count();
            if zeros != 0 {
                let zeros_f64 = zeros as f64;
                corrected_estimate = m_f64 * (m_f64 / zeros_f64).ln();
            } else {
                corrected_estimate = estimate;
            }
        } else if estimate <= ((1_f64 / 30_f64) * (two_32)) {
            corrected_estimate = estimate;
        } else {
            corrected_estimate = -(two_32) * (1_f64 - estimate / two_32).ln();
        }
        corrected_estimate
    }

    pub fn union(&self, other: &HyperLogLog) {
        self.registers.write().unwrap().iter_mut().zip(other.registers.read().unwrap().iter()).for_each(|zipped| {
            *zipped.0 = cmp::max(*zipped.0, *zipped.1);
        });
    }

    pub fn set_registers(&self, other: &HyperLogLog) {
        self.registers.write().unwrap().iter_mut().zip(other.registers.read().unwrap().iter()).for_each(|zipped| {
            *zipped.0 = *zipped.1;
        });
    }

    fn estimate(&self) -> f64 {
        let mut sum: f64 = 0.0;
        let registers = self.registers.read().unwrap();
        for i in 0..self.m {
            sum += 2_f64.powi(-(registers[i as usize] as i32))
        }
        let m2: f64 = (self.m * self.m) as f64;
        (self.alpha * m2) / sum
    }

    fn zero_count(&self) -> u32 {
        self.registers.read().unwrap().iter().filter(|r| **r == 0).count() as u32
    }

    fn first_set_bit(&self, value: u64) -> u8 {
        let zeros = value.leading_zeros() as u8;
        zeros + 1
    }

    fn compute_alpha(p: u8) -> f64 {
        match p {
            4 => 0.673,
            5 => 0.987,
            6 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / ((1 << p) as f64))
        }
    }
}

impl Clone for HyperLogLog {
    fn clone(&self) -> HyperLogLog {
        HyperLogLog {
            p: self.p,
            m: self.m,
            registers: Arc::new(RwLock::new(self.registers.read().unwrap().clone())),
            alpha: self.alpha
        }
    }
}

impl PartialEq for HyperLogLog {
    fn eq(&self, other: &HyperLogLog) -> bool {
        self.registers.read().unwrap().iter().zip(other.registers.read().unwrap().iter()).all(|r| *r.0 == *r.1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set_values(counter: &mut HyperLogLog, values: &[u32]) {
        values.iter().for_each(|x| {
            counter.add(*x as u32);
        });
    }

    fn count_values(values: &[u32]) -> f64 {
        let mut counter = HyperLogLog::new(8);
        set_values(&mut counter, &values);
        counter.count()
    }

    #[test]
    fn empty() {
        let values = [];
        assert!(count_values(&values).round() < 1e-6);
    }

    #[test]
    fn basic_case() {
        let values = [32, 64, 100];
        assert!(count_values(&values).round() == 3_f64);
    }

    #[test]
    fn basic_case_2() {
        let values = [3200, 1, 2, 2, 3, 10000];
        assert!(count_values(&values).round() == 5_f64)
    }

    #[test]
    fn union() {
        let values1 = [1, 2, 3];
        let values2 = [1, 4, 5, 6];
        let mut counter1 = HyperLogLog::new(8);
        let mut counter2 = HyperLogLog::new(8);
        set_values(&mut counter1, &values1);
        set_values(&mut counter2, &values2);
        counter1.union(&counter2);
        assert!(counter1.count().round() == 6_f64);
    }
}
