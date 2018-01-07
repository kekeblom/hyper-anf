extern crate rayon;
extern crate num_bigint;
extern crate csv;
mod hyperloglog;

use std::env;
use std::fs::File;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io::{BufReader, BufRead};
use std::error::Error;
use rayon::prelude::*;
use num_bigint::BigUint;
use hyperloglog::HyperLogLog;

fn init_counters<'a>(nodes: &Vec<&'a BigUint>, b: u8) -> HashMap<&'a BigUint, hyperloglog::HyperLogLog> {
    let mut counters: HashMap<&'a BigUint, hyperloglog::HyperLogLog> = HashMap::with_capacity(nodes.len());
    for node in nodes.iter() {
        let mut counter = HyperLogLog::new(b);
        counter.add(node);
        counters.insert(node, counter);
    }
    counters
}

struct Graph<'a> {
    nodes: HashMap<&'a BigUint, Vec<&'a BigUint>>
}

fn add<'a>(nodes: &mut HashMap<&'a BigUint, Vec<&'a BigUint>>, from: &'a BigUint, to: &'a BigUint) {
    match nodes.entry(from) {
        Entry::Vacant(e) => {
            e.insert(vec![to]);
        },
        Entry::Occupied(mut e) => {
            e.get_mut().push(to);
        }
    }
}

impl<'a> Graph<'a> {
    pub fn new(vertices: &Vec<(BigUint, BigUint)>) -> Graph {
        let references: Vec<_> = vertices.iter().map(|vertex| {
            (&vertex.0, &vertex.1)
        }).collect();
        let nodes: HashMap<_, _> = references.iter().fold(HashMap::with_capacity(vertices.len()), |mut acc, &vertex| {
            add(&mut acc, vertex.0, vertex.1);
            add(&mut acc, vertex.1, vertex.0);
            acc
        });
        Graph { nodes: nodes }
    }

    pub fn get_nodes(&self) -> Vec<&'a BigUint> {
        self.nodes.keys().map(|x| *x).collect::<Vec<&BigUint>>()
    }
}

struct Arguments {
    in_file: String,
    out_file: String,
    b: u8
}

fn read_args() -> Arguments {
    let args = env::args().collect::<Vec<String>>();
    let in_file: String = args.get(1).expect("No input file name given").clone();
    let out_file: String = args.get(2).unwrap_or(&String::from("./out.csv")).clone();
    let b: u8 = args.get(3).unwrap_or(&String::from("10")).parse::<u8>().unwrap();
    Arguments { in_file: in_file, out_file: out_file, b: b }
}

fn read_vertices(file_name: &String) -> Vec<(BigUint, BigUint)> {
    let f = File::open(file_name).expect("File does not exist.");
    let file = BufReader::new(&f);
    file.lines().map(|r| r.unwrap()).filter(|l| {
            let mut chars = l.chars();
            match chars.nth(0) {
                Some('#') => false,
                _ => true
            }
        }).map(|line| {
        let mut vertex = line.split_whitespace().map(|value| {
            value.parse::<BigUint>().unwrap()
        });
        let from = vertex.next().unwrap();
        let to = vertex.next().unwrap();
        (to, from)
    }).collect::<Vec<_>>()
}

fn write_counters(writer: &mut csv::Writer<std::fs::File>, counters: &HashMap<&BigUint, HyperLogLog>, t: u8) -> Result<(), Box<Error>> {
    for (node_id, counter) in counters.iter() {
        let node_string = node_id.to_str_radix(10);
        writer.serialize((t, node_string, counter.count()))?;
    }
    writer.flush()?;
    Ok(())
}

fn set_values(counters_to_set: &HashMap<&BigUint, HyperLogLog>, to_copy: &HashMap<&BigUint, HyperLogLog>) {
    counters_to_set.values().zip(to_copy.values()).collect::<Vec<_>>().par_iter().for_each(|key_value| {
        let counter_to_set = key_value.0;
        let to_copy = key_value.1;
        counter_to_set.set_registers(to_copy);
    });
}

fn count_values(c: &HashMap<&BigUint, HyperLogLog>) -> f64 {
    c.par_iter().map(|key_value| key_value.1.count()).sum()
}

fn main() {
    let args = read_args();
    let vertices: &Vec<(BigUint, BigUint)> = &read_vertices(&args.in_file);
    let graph = Graph::new(vertices);
    println!("Nodes: {}", graph.nodes.keys().len());

    let prev_counters = init_counters(&graph.get_nodes(), args.b);
    let mut t = 0_u8;
    let mut csv_writer = csv::Writer::from_path(args.out_file).expect("Out can't be opened");
    let current_counters = prev_counters.clone();
    loop {
        match write_counters(&mut csv_writer, &current_counters, t) {
            Ok(_) => println!("wrote counters."),
            Err(e) => println!("{}", e)
        }
        println!("t = {t}, {sum}", t=t, sum=count_values(&current_counters));
        graph.nodes.par_iter().for_each(|key_value| {
            let node_id = key_value.0;
            let neighbors = key_value.1;
            let node_counter = current_counters.get(node_id).unwrap();
            for neighbor in neighbors {
                node_counter.union(&prev_counters.get(neighbor).unwrap());
            }
        });
        let all_same = current_counters.values().zip(prev_counters.values())
            .collect::<Vec<_>>()
            .par_iter()
            .all(|c| c.0 == c.1);
        if all_same {
            break;
        }
        set_values(&prev_counters, &current_counters);
        t += 1;
    }
}
