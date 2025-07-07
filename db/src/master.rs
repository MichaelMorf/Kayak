/* Copyright (c) 2018 University of Utah
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

use bincode::serialize;
use crypto::bcrypt::bcrypt;
use hashbrown::HashMap;

use std::fs::File;
use std::io::Write;
use std::mem::{size_of, transmute};
use std::pin::Pin;
use std::rc::Rc;
use std::str::from_utf8;
use std::str::FromStr;
use std::sync::Arc;

use super::alloc::Allocator;
use super::bytes::Bytes;
use super::container::Container;
use super::context::Context;
use super::native::Native;
use super::rpc::parse_record_optype;
use super::service::Service;
use super::table::Version;
use super::task::{Task, TaskPriority};
use super::tenant::Tenant;
use super::tx::TX;
use super::wireformat::*;

use util::common::TESTING_DATASET;
use util::model::{get_raw_data, insert_global_model, run_ml_application};

use e2d2::common::EmptyMetadata;
use e2d2::headers::UdpHeader;
use e2d2::interface::Packet;
use spin::RwLock;

use sandstorm::common::{TableId, TenantId, PACKET_UDP_LEN};
use sandstorm::db::DB;
use sandstorm::ext::*;
use sandstorm::pack::pack;
use sandstorm::{LittleEndian, ReadBytesExt};

/// Convert a raw pointer for Allocator into a Allocator reference. This can be used to pass
/// the allocator reference across closures without cloning the allocator object.
pub fn accessor<'a>(alloc: *const Allocator) -> &'a Allocator {
    unsafe { &*alloc }
}

// The number of buckets in the `tenants` hashtable inside of Master.
const TENANT_BUCKETS: usize = 32;

/// The primary service in Sandstorm. Master is responsible managing tenants, extensions, and
/// the database. It implements the Service trait, allowing it to generate schedulable tasks
/// for data and extension related RPC requests.
pub struct Master {
    /// A Map of all tenants in the system. Since Sandstorm is a multi-tenant system, most RPCs
    /// will require a lookup on this map.
    tenants: [RwLock<HashMap<TenantId, Arc<Tenant>>>; TENANT_BUCKETS],

    /// An extension manager maintaining state concerning extensions loaded into the system.
    /// Required to retrieve and determine if an extension belongs to a particular tenant while
    /// handling an invocation request.
    pub extensions: ExtensionManager,

    /// Manager of the table heap. Required to allow writes to the database.
    heap: Allocator,
}

// Implementation of methods on Master.
impl Master {
    /// Creates and returns a new Master service.
    ///
    /// # Return
    ///
    /// A Master service capable of creating schedulable tasks out of RPC requests.
    pub fn new() -> Master {
        Master {
            // Cannot use copy constructor because of the Arc<Tenant>.
            tenants: array_init::array_init(|_| RwLock::new(HashMap::new())),
            extensions: ExtensionManager::new(),
            heap: Allocator::new(),
        }
    }

    /// Adds a tenant and a table full of objects.
    ///
    /// # Arguments
    ///
    /// * `tenant_id`: Identifier of the tenant to be added. Any existing tenant with the same
    ///                identifier will be overwritten.
    /// * `table_id`:  Identifier of the table to be added to the tenant. This table will contain
    ///                all the objects.
    /// * `num`:       The number of objects to be added to the data table.
    pub fn fill_test(&self, tenant_id: TenantId, table_id: TableId, num: u32) {
        // Create a tenant containing the table.
        let tenant = Tenant::new(tenant_id);
        tenant.create_table(table_id);

        let table = tenant
            .get_table(table_id)
            .expect("Failed to init test table.");

        let mut key = vec![0; 30];
        let mut val = vec![0; 100];

        // Allocate objects, and fill up the above table. Each object consists of a 30 Byte key
        // and a 100 Byte value.
        for i in 1..(num + 1) {
            let value: [u8; 4] = unsafe { transmute(((i + 1) % (num + 1)).to_le()) };
            let temp: [u8; 4] = unsafe { transmute(i.to_le()) };
            &key[0..4].copy_from_slice(&temp);
            &val[0..4].copy_from_slice(&value);

            let obj = self
                .heap
                .object(tenant_id, table_id, &key, &val)
                .expect("Failed to create test object.");
            table.put(obj.0, obj.1);
        }
        // Add the tenant.
        self.insert_tenant(tenant);
    }

    /// Populates the TAO dataset.
    ///
    /// # Arguments
    ///
    /// * `tenant_id`: Identifier of the tenant to be added. Any existing tenant with the same
    ///                identifier will be overwritten.
    /// * `num`:       The number of objects to be added to the data table.
    pub fn fill_tao(&self, tenant_id: TenantId, num: u32) {
        // Create a tenant containing two tables, one for objects, and one for
        // associations.
        let tenant = Tenant::new(tenant_id);
        tenant.create_table(1); // Holds tao objects.
        tenant.create_table(2); // Holds tao assocs.

        // First, fill up the object table.
        let table = tenant.get_table(1).expect("Failed to init test table.");

        // Objects are identified by an 8 byte key.
        let mut key = vec![0; 8];
        // Objects contain a 4 byte otype, 8 byte version, 4 byte update time, and
        // 16 byte payload, all of which are zero.
        let val = vec![0; 32];

        // Setup the object table with num objects.
        for i in 1..(num + 1) {
            let temp: [u8; 4] = unsafe { transmute(i.to_le()) };
            &key[0..4].copy_from_slice(&temp);

            let obj = self
                .heap
                .object(tenant_id, 1, &key, &val)
                .expect("Failed to create test object.");
            table.put(obj.0, obj.1);
        }

        // Next, fill up the assoc table.
        let table = tenant.get_table(2).expect("Failed to init test table.");

        // Assocs are identified by an 8 byte object 1 id, 2 byte association
        // type (always zero), and 8 byte object 2 id.
        let mut key = vec![0; 18];
        // Assocs have a 22 byte value (all zeros).
        let val = vec![0; 22];

        // Populate the assoc table. Each object gets four assocs to it's
        // neighbours.
        for i in 1..(num + 1) {
            let temp: [u8; 4] = unsafe { transmute(i.to_le()) };
            &key[0..4].copy_from_slice(&temp);

            // Assoc list for this particular object.
            let mut list: Vec<u8> = Vec::new();

            for a in 1u32..5u32 {
                let temp: [u8; 4] = unsafe { transmute(((i + a) % num).to_le()) };
                &key[10..14].copy_from_slice(&temp);
                list.extend_from_slice(&temp);
                list.extend_from_slice(&[0; 12]);

                // Add this assoc to the assoc table.
                let obj = self
                    .heap
                    .object(tenant_id, 2, &key, &val)
                    .expect("Failed to create test object.");
                table.put(obj.0, obj.1);
            }

            // Add the assoc list to the table too.
            let obj = self
                .heap
                .object(tenant_id, 2, &key[0..10], &list)
                .expect("Failed to create test object.");
            table.put(obj.0, obj.1);
        }

        // Add the tenant.
        self.insert_tenant(tenant);
    }

    /// Populates the aggregate dataset.
    ///
    /// # Arguments
    ///
    /// * `tenant_id`: Identifier of the tenant to be added. Any existing tenant with the same
    ///                identifier will be overwritten.
    /// * `table_id`:  Identifier of the table to be added to the tenant. This table will contain
    ///                all the objects.
    /// * `num`:       The number of objects to be added to the data table.
    pub fn fill_aggregate(&self, tenant_id: TenantId, table_id: TableId, num: u32) {
        // One table for the tenant. Both, objects and indirection lists will be
        // stored in here.
        let tenant = Tenant::new(tenant_id);
        tenant.create_table(table_id);

        let table = tenant
            .get_table(table_id)
            .expect("Failed to init test table.");

        // The number of records per aggregation.
        const N_AGG: u32 = 12;
        // The length of each record's key.
        const K_LEN: u32 = 30;
        // The length of each record's value.
        const V_LEN: u32 = 100;

        // The total number of records.
        let records: u32 = 4 * (num + 1);

        // First, add in the indirection records. Keys are 8 bytes, and values are
        // lists of 30 Byte keys.
        for i in 1..(num + 1) {
            let mut key = vec![0; 8];
            let mut val = vec![];

            let temp: [u8; 4] = unsafe { transmute(i.to_le()) };
            &key[0..4].copy_from_slice(&temp);

            for e in 0..N_AGG {
                let mut k = vec![0; K_LEN as usize];
                let t: [u8; 4] = unsafe { transmute(((i * N_AGG + e) % records).to_le()) };
                &k[0..4].copy_from_slice(&t);

                val.extend_from_slice(&k);
            }

            let obj = self
                .heap
                .object(tenant_id, table_id, &key, &val)
                .expect("Failed to create test object.");
            table.put(obj.0, obj.1);
        }

        // Next, populate the actual records.
        for i in 0..records {
            let mut key = vec![0; K_LEN as usize];
            let mut val = vec![0; V_LEN as usize];

            let temp: [u8; 4] = unsafe { transmute(i.to_le()) };
            &key[0..4].copy_from_slice(&temp);
            &val[0..4].copy_from_slice(&temp);

            let obj = self
                .heap
                .object(tenant_id, table_id, &key, &val)
                .expect("Failed to create test object.");
            table.put(obj.0, obj.1);
        }

        self.insert_tenant(tenant);
    }

    /// Adds a tenant and a table full of objects.
    ///
    /// # Arguments
    ///
    /// * `num_tenants`: The number of tenants for this workload.
    pub fn fill_analysis(&self, num_tenants: u32) {
        // Run the ML model required for the extension and store the serialized version
        // and deserialized version of the model, which will be used in the extension.
        let (sgd, d_tree, r_forest) = run_ml_application();
        insert_global_model(
            String::from("analysis"),
            sgd.clone(),
            d_tree.clone(),
            r_forest.clone(),
        );

        let table_id = 1;
        let data = get_raw_data(TESTING_DATASET);

        for tenant_id in 1..(num_tenants + 1) {
            // Create a tenant containing the table.
            let tenant = Tenant::new(tenant_id);
            tenant.create_table(table_id);

            let table = tenant
                .get_table(table_id)
                .expect("Failed to init test table.");

            let mut key = vec![0; 30];
            for (row, line) in data.lines().enumerate() {
                // Prepare the key for the record.
                let temp: [u8; 4] = unsafe { transmute(((row + 1) as u32).to_le()) };
                &key[0..4].copy_from_slice(&temp);

                // Prepare the value for the record.
                let mut value: Vec<f32> = Vec::new();
                for col_str in line.split_whitespace() {
                    value.push(f32::from_str(col_str).unwrap());
                }
                let serialized = serialize(&value).unwrap();

                // Insert the key-value in the table.
                let obj = self
                    .heap
                    .object(tenant_id, table_id, &key, &serialized)
                    .expect("Failed to create test object.");
                table.put(obj.0, obj.1);
            }

            // Add the tenant.
            self.insert_tenant(tenant);
        }
    }

    /// Populates the authentication dataset.
    ///
    /// # Arguments
    ///
    /// * `tenant_id`: Identifier of the tenant to be added. Any existing tenant with the same
    ///                identifier will be overwritten.
    /// * `table_id`:  Identifier of the table to be added to the tenant. This table will contain
    ///                all the objects.
    /// * `num`:       The number of objects to be added to the data table.
    pub fn fill_auth(&self, tenant_id: TenantId, table_id: TableId, num: u32) {
        // Create a tenant containing the table.
        let tenant = Tenant::new(tenant_id);
        tenant.create_table(table_id);

        let table = tenant
            .get_table(table_id)
            .expect("Failed to init test table.");

        let mut username = vec![0; 30];
        let mut password = vec![0; 72];
        let mut hash_salt = vec![0; 40];
        let mut salt = vec![0; 16];

        // Allocate objects, and fill up the above table. Each object consists of a 30 Byte key
        // and a 40 Byte value(24 byte HASH followed by 16 byte SALT).
        for i in 1..(num + 1) {
            let temp: [u8; 4] = unsafe { transmute(i.to_le()) };
            &username[0..4].copy_from_slice(&temp);
            &password[0..4].copy_from_slice(&temp);
            &hash_salt[24..28].copy_from_slice(&temp);
            &salt[0..4].copy_from_slice(&temp);

            let output: &mut [u8] = &mut [0; 24];
            bcrypt(1, &salt, &password, output);
            &hash_salt[0..24].copy_from_slice(&output);

            // Add a mapping of the username and (HASH+SALT) in the table.
            let obj = self
                .heap
                .object(tenant_id, table_id, &username, &hash_salt)
                .expect("Failed to create test object.");
            table.put(obj.0, obj.1);
        }

        // Add the tenant.
        self.insert_tenant(tenant);
    }

    /// Populates the ANALYSIS, AUTH, and PUSHBACK OR YCSB dataset.
    ///
    /// # Arguments
    ///
    /// * `num_tenants`: The number of tenants for this workload.
    /// * `num`:       The number of objects to be added to the data table.
    pub fn fill_mix(&self, num_tenants: u32, num: u32) {
        let tao_table_id1 = 1;
        let tao_table_id2 = 2;
        let ml_table_id = 3;

        // Run the ML model required for the extension and store the serialized version
        // and deserialized version of the model, which will be used in the extension.
        let (sgd, d_tree, r_forest) = run_ml_application();
        insert_global_model(
            String::from("analysis"),
            sgd.clone(),
            d_tree.clone(),
            r_forest.clone(),
        );

        let data = get_raw_data(TESTING_DATASET);

        for tenant_id in 1..(num_tenants + 1) {
            // Create a tenant containing the table.
            let tenant = Tenant::new(tenant_id);
            tenant.create_table(tao_table_id1);
            tenant.create_table(tao_table_id2);
            tenant.create_table(ml_table_id);

            //-----------------------Fill TAO----------------------------------------------------------//
            // First, fill up the object table.
            let table = tenant
                .get_table(tao_table_id1)
                .expect("Failed to init test table.");

            // Objects are identified by an 8 byte key.
            let mut key = vec![0; 8];
            // Objects contain a 4 byte otype, 8 byte version, 4 byte update time, and
            // 16 byte payload, all of which are zero.
            let val = vec![0; 32];

            // Setup the object table with num objects.
            for i in 1..(num + 1) {
                let temp: [u8; 4] = unsafe { transmute(i.to_le()) };
                &key[0..4].copy_from_slice(&temp);

                let obj = self
                    .heap
                    .object(tenant_id, tao_table_id1, &key, &val)
                    .expect("Failed to create test object.");
                table.put(obj.0, obj.1);
            }

            // Next, fill up the assoc table.
            let table = tenant
                .get_table(tao_table_id2)
                .expect("Failed to init test table.");

            // Assocs are identified by an 8 byte object 1 id, 2 byte association
            // type (always zero), and 8 byte object 2 id.
            let mut key = vec![0; 18];
            // Assocs have a 22 byte value (all zeros).
            let val = vec![0; 22];

            // Populate the assoc table. Each object gets four assocs to it's
            // neighbours.
            for i in 1..(num + 1) {
                let temp: [u8; 4] = unsafe { transmute(i.to_le()) };
                &key[0..4].copy_from_slice(&temp);

                // Assoc list for this particular object.
                let mut list: Vec<u8> = Vec::new();

                for a in 1u32..5u32 {
                    let temp: [u8; 4] = unsafe { transmute(((i + a) % num).to_le()) };
                    &key[10..14].copy_from_slice(&temp);
                    list.extend_from_slice(&temp);
                    list.extend_from_slice(&[0; 12]);

                    // Add this assoc to the assoc table.
                    let obj = self
                        .heap
                        .object(tenant_id, tao_table_id2, &key, &val)
                        .expect("Failed to create test object.");
                    table.put(obj.0, obj.1);
                }

                // Add the assoc list to the table too.
                let obj = self
                    .heap
                    .object(tenant_id, tao_table_id2, &key[0..10], &list)
                    .expect("Failed to create test object.");
                table.put(obj.0, obj.1);
            }

            //-----------------------Fill ANALYSIS----------------------------------------------------------//
            let table = tenant
                .get_table(ml_table_id)
                .expect("Failed to init test table.");

            let mut key = vec![0; 30];
            for (row, line) in data.lines().enumerate() {
                // Prepare the key for the record.
                let temp: [u8; 4] = unsafe { transmute(((row + 1) as u32).to_le()) };
                &key[0..4].copy_from_slice(&temp);

                // Prepare the value for the record.
                let mut value: Vec<f32> = Vec::new();
                for col_str in line.split_whitespace() {
                    value.push(f32::from_str(col_str).unwrap());
                }
                let serialized = serialize(&value).unwrap();

                // Insert the key-value in the table.
                let obj = self
                    .heap
                    .object(tenant_id, ml_table_id, &key, &serialized)
                    .expect("Failed to create test object.");
                table.put(obj.0, obj.1);
            }
            // Add the tenant.
            self.insert_tenant(tenant);
        }
    }

    /// Adds a tenant and a table full of objects.
    ///
    /// # Arguments
    ///
    /// * `tenant_id`: Identifier of the tenant to be added. Any existing tenant with the same
    ///                identifier will be overwritten.
    /// * `table_id`:  Identifier of the table to be added to the tenant. This table will contain
    ///                all the objects.
    /// * `num`:       The number of objects to be added to the data table.
    pub fn fill_ycsb(&self, tenant_id: TenantId, table_id: TableId, num: u32) {
        // Create a tenant containing the table.
        let tenant = Tenant::new(tenant_id);
        tenant.create_table(table_id);

        let table = tenant
            .get_table(table_id)
            .expect("Failed to init test table.");

        let mut key = vec![0; 30];
        let mut val = vec![0; 100];

        // Allocate objects, and fill up the above table. Each object consists of a 30 Byte key
        // and a 100 Byte value.
        for i in 1..(num + 1) {
            let value: [u8; 4] = unsafe { transmute(255u32.to_le()) };
            let temp: [u8; 4] = unsafe { transmute(i.to_le()) };
            &key[0..4].copy_from_slice(&temp);
            &val[0..4].copy_from_slice(&value);

            let obj = self
                .heap
                .object(tenant_id, table_id, &key, &val)
                .expect("Failed to create test object.");
            table.put(obj.0, obj.1);
        }

        // Add the tenant.
        self.insert_tenant(tenant);
    }

    /// Loads the get() extension.
    ///
    /// # Arguments
    ///
    /// * `tenant`: Identifier of the tenant to load the extension for.
    pub fn load_test(&self, tenant: TenantId) {
        // Load the get() extension.
        let name = "../ext/get/target/release/libget.so";
        if self.extensions.load(name, tenant, "get") == false {
            panic!("Failed to load get() extension.");
        }

        // Load the put() extension.
        let name = "../ext/put/target/release/libput.so";
        if self.extensions.load(name, tenant, "put") == false {
            panic!("Failed to load put() extension.");
        }

        // Load the tao() extension.
        let name = "../ext/tao/target/release/libtao.so";
        if self.extensions.load(name, tenant, "tao") == false {
            panic!("Failed to load tao() extension.");
        }

        // Load the bad() extension.
        let name = "../ext/bad/target/release/libbad.so";
        if self.extensions.load(name, tenant, "bad") == false {
            panic!("Failed to load bad() extension.");
        }

        // Load the long() extension.
        let name = "../ext/long/target/release/liblong.so";
        if self.extensions.load(name, tenant, "long") == false {
            panic!("Failed to load long() extension.");
        }

        // Load the aggregate() extension.
        let name = "../ext/aggregate/target/release/libaggregate.so";
        if self.extensions.load(name, tenant, "aggregate") == false {
            panic!("Failed to load aggregate() extension.");
        }

        // Load the pushback() extension.
        let name = "../ext/pushback/target/release/libpushback.so";
        if self.extensions.load(name, tenant, "pushback") == false {
            panic!("Failed to load pushback() extension.");
        }

        // Load the scan() extension.
        let name = "../ext/scan/target/release/libscan.so";
        if self.extensions.load(name, tenant, "scan") == false {
            panic!("Failed to load scan() extension.");
        }

        // Load the analysis() extension.
        let name = "../ext/analysis/target/release/libanalysis.so";
        if self.extensions.load(name, tenant, "analysis") == false {
            panic!("Failed to load analysis() extension.");
        }

        // Load the auth() extension.
        let name = "../ext/auth/target/release/libauth.so";
        if self.extensions.load(name, tenant, "auth") == false {
            panic!("Failed to load auth() extension.");
        }

        // Load the ycsbt() extension.
        let name = "../ext/ycsbt/target/release/libycsbt.so";
        if self.extensions.load(name, tenant, "ycsbt") == false {
            panic!("Failed to load ycsbt() extension.");
        }

        // Load the checksum() extension.
        let name = "../ext/checksum/target/release/libchecksum.so";
        if self.extensions.load(name, tenant, "checksum") == false {
            panic!("Failed to load checksum() extension.");
        }
    }

    /// Loads the get(), put(), and tao() extensions once, and shares them across multiple tenants.
    ///
    /// # Arguments
    ///
    /// * `tenants`: The number of tenants that should share the above three extensions.
    pub fn load_test_shared(&self, tenants: u32) {
        // First, load up the get, put, and tao extensions for tenant 1.
        self.load_test(0);

        // Next, share these extensions with the other tenants.
        for tenant in 1..tenants {
            // Share the get() extension.
            if self.extensions.share(0, tenant, "get") == false {
                panic!("Failed to share get() extension.");
            }

            // Share the put() extension.
            if self.extensions.share(0, tenant, "put") == false {
                panic!("Failed to share put() extension.");
            }

            // Share the tao() extension.
            if self.extensions.share(0, tenant, "tao") == false {
                panic!("Failed to share tao() extension.");
            }
        }
    }

    /// This method returns a handle to a tenant if it exists.
    ///
    /// # Arguments
    ///
    /// * `tenant_id`: The identifier for the tenant to be returned.
    ///
    /// # Return
    ///
    /// An atomic reference counted handle to the tenant if it exists.
    fn get_tenant(&self, tenant_id: TenantId) -> Option<Arc<Tenant>> {
        // Acquire a read lock. The bucket is determined by the least significant byte of the
        // tenant id.
        let bucket = (tenant_id & 0xff) as usize & (TENANT_BUCKETS - 1);
        let map = self.tenants[bucket].read();

        // Lookup, and return the tenant if it exists.
        map.get(&tenant_id)
            .and_then(|tenant| Some(Arc::clone(tenant)))
    }

    /// This method adds a tenant to Master.
    ///
    /// # Arguments
    ///
    /// * `tenant`: The tenant to be added.
    fn insert_tenant(&self, tenant: Tenant) {
        // Acquire a write lock. The bucket is determined by the least significant byte of the
        // tenant id.
        let bucket = (tenant.id() & 0xff) as usize & (TENANT_BUCKETS - 1);
        let mut map = self.tenants[bucket].write();

        // Insert the tenant and return.
        map.insert(tenant.id(), Arc::new(tenant));
    }

    /// Handles the Get() RPC request.
    ///
    /// A hash table lookup is performed on a supplied tenant id, table id, and key. If successfull,
    /// the result of the lookup is written into a response packet, and the response header is
    /// updated. In the case of a failure, the response header is updated with a status indicating
    /// the reason for the failure.
    ///
    /// # Arguments
    ///
    /// * `req`: The RPC request packet sent by the client, parsed upto it's UDP header.
    /// * `res`: The RPC response packet, with pre-allocated headers upto UDP.
    ///
    /// # Return
    ///
    /// A Native task that can be scheduled by the database. In the case of an error, the passed
    /// in request and response packets are returned with the response status appropriately set.
    #[allow(unreachable_code)]
    #[allow(unused_assignments)]
    fn get(
        &self,
        req: Packet<UdpHeader, EmptyMetadata>,
        res: Packet<UdpHeader, EmptyMetadata>,
    ) -> Result<
        Box<Task>,
        (
            Packet<UdpHeader, EmptyMetadata>,
            Packet<UdpHeader, EmptyMetadata>,
        ),
    > {
        // First, parse the request packet.
        let req = req.parse_header::<GetRequest>();

        // Read fields off the request header.
        let mut tenant_id: TenantId = 0;
        let mut table_id: TableId = 0;
        let mut key_length = 0;
        let mut rpc_stamp = 0;

        {
            let hdr = req.get_header();
            tenant_id = hdr.common_header.tenant as TenantId;
            table_id = hdr.table_id as TableId;
            key_length = hdr.key_length;
            rpc_stamp = hdr.common_header.stamp;
        }

        // Next, add a header to the response packet.
        let mut res = res
            .push_header(&GetResponse::new(
                rpc_stamp,
                OpCode::SandstormGetRpc,
                tenant_id,
            ))
            .expect("Failed to setup GetResponse");

        // If the payload size is less than the key length, return an error.
        if req.get_payload().len() < key_length as usize {
            res.get_mut_header().common_header.status = RpcStatus::StatusMalformedRequest;
            return Err((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ));
        }

        // Lookup the tenant, and get a handle to the allocator. Required to avoid capturing a
        // reference to Master in the generator below.
        let tenant = self.get_tenant(tenant_id);
        let alloc: *const Allocator = &self.heap;
        let mut status: RpcStatus = RpcStatus::StatusTenantDoesNotExist;
        let optype: u8 = 0x1; // OpType::SandstormRead
        let mut res = res;
        let outcome =
            tenant.and_then(|tenant| {
                status = RpcStatus::StatusTableDoesNotExist;
                tenant.get_table(table_id)
            })
            .and_then(|table| {
                status = RpcStatus::StatusObjectDoesNotExist;
                let (key, _) = req.get_payload().split_at(key_length as usize);
                table.get(key)
            })
            .and_then(|entry| {
                status = RpcStatus::StatusInternalError;
                let alloc: &Allocator = accessor(alloc);
                Some((alloc.resolve(entry.value), entry.version))
            })
            .and_then(|(opt, version)| {
                if let Some(opt) = opt {
                    let (k, value) = &opt;
                    status = RpcStatus::StatusInternalError;
                    let _result = res.add_to_payload_tail(1, pack(&optype));
                    let _ = res.add_to_payload_tail(size_of::<Version>(), &unsafe { transmute::<Version, [u8; 8]>(version) });
                    let result = res.add_to_payload_tail(k.len(), &k[..]);
                    match result {
                        Ok(()) => {
                            res.add_to_payload_tail(value.len(), &value[..]).ok()
                        }
                        Err(_) => {
                            Some(())
                        }
                    }
                } else {
                    None
                }
            })
            .and_then(|_| {
                status = RpcStatus::StatusOk;
                Some(())
            });
        match outcome {
            Some(()) => {
                let val_len = res.get_payload().len() as u32;
                let hdr: &mut GetResponse = res.get_mut_header();
                hdr.value_length = val_len;
                hdr.common_header.status = status;
            }
            None => {
                res.get_mut_header().common_header.status = status;
            }
        }
        Ok(Box::new(Native::new(TaskPriority::REQUEST, Box::pin(move || {
            Some((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ))
        }))))
    }

    /// Handles the Put() RPC request.
    ///
    /// A hash table lookup is performed on a supplied tenant id, table id, and key. If successfull,
    /// the result of the lookup is written into a response packet, and the response header is
    /// updated. In the case of a failure, the response header is updated with a status indicating
    /// the reason for the failure.
    ///
    /// # Arguments
    ///
    /// * `req`: The RPC request packet sent by the client, parsed upto it's UDP header.
    /// * `res`: The RPC response packet, with pre-allocated headers upto UDP.
    ///
    /// # Return
    ///
    /// A Native task that can be scheduled by the database. In the case of an error, the passed
    /// in request and response packets are returned with the response status appropriately set.
    #[allow(unreachable_code)]
    #[allow(unused_assignments)]
    fn put(
        &self,
        req: Packet<UdpHeader, EmptyMetadata>,
        res: Packet<UdpHeader, EmptyMetadata>,
    ) -> Result<
        Box<Task>,
        (
            Packet<UdpHeader, EmptyMetadata>,
            Packet<UdpHeader, EmptyMetadata>,
        ),
    > {
        // First, parse the request packet.
        let req = req.parse_header::<PutRequest>();

        // Read fields off the request header.
        let mut tenant_id: TenantId = 0;
        let mut table_id: TableId = 0;
        let mut key_length = 0;
        let mut value_length = 0;
        let mut rpc_stamp = 0;

        {
            let hdr = req.get_header();
            tenant_id = hdr.common_header.tenant as TenantId;
            table_id = hdr.table_id as TableId;
            key_length = hdr.key_length;
            value_length = hdr.value_length;
            rpc_stamp = hdr.common_header.stamp;
        }

        // Next, add a header to the response packet.
        let mut res = res
            .push_header(&PutResponse::new(
                rpc_stamp,
                OpCode::SandstormPutRpc,
                tenant_id,
            ))
            .expect("Failed to setup PutResponse");

        // If the payload size is less than the key length, return an error.
        if req.get_payload().len() < key_length as usize {
            res.get_mut_header().common_header.status = RpcStatus::StatusMalformedRequest;
            return Err((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ));
        }

        // Lookup the tenant, and get a handle to the allocator. Required to avoid capturing a
        // reference to Master in the generator below.
        let tenant = self.get_tenant(tenant_id);
        let alloc: *const Allocator = &self.heap;
        let mut status: RpcStatus = RpcStatus::StatusTenantDoesNotExist;
        let optype: u8 = 0x2; // OpType::SandstormWrite
        let mut res = res;
        let outcome =
            tenant.and_then(|tenant| {
                status = RpcStatus::StatusTableDoesNotExist;
                tenant.get_table(table_id)
            })
            .and_then(|table| {
                // If the payload size is less than the key length + value length, return an error.
                if req.get_payload().len() < (key_length + value_length) as usize {
                    res.get_mut_header().common_header.status = RpcStatus::StatusMalformedRequest;
                    return None;
                }

                status = RpcStatus::StatusObjectDoesNotExist;
                let (key, value) = req.get_payload().split_at(key_length as usize);
                table.put(key, value)
            })
            .and_then(|entry| {
                status = RpcStatus::StatusInternalError;
                let alloc: &Allocator = accessor(alloc);
                Some((alloc.resolve(entry.value), entry.version))
            })
            .and_then(|(opt, version)| {
                if let Some(opt) = opt {
                    let (k, value) = &opt;
                    status = RpcStatus::StatusInternalError;
                    let _result = res.add_to_payload_tail(1, pack(&optype));
                    let _ = res.add_to_payload_tail(size_of::<Version>(), &unsafe { transmute::<Version, [u8; 8]>(version) });
                    let result = res.add_to_payload_tail(k.len(), &k[..]);
                    match result {
                        Ok(()) => {
                            res.add_to_payload_tail(value.len(), &value[..]).ok()
                        }
                        Err(_) => {
                            Some(())
                        }
                    }
                } else {
                    None
                }
            })
            .and_then(|_| {
                status = RpcStatus::StatusOk;
                Some(())
            });
        match outcome {
            Some(()) => {
                let val_len = res.get_payload().len() as u32;
                let hdr: &mut PutResponse = res.get_mut_header();
                hdr.value_length = val_len;
                hdr.common_header.status = status;
            }
            None => {
                res.get_mut_header().common_header.status = status;
            }
        }
        Ok(Box::new(Native::new(TaskPriority::REQUEST, Box::pin(move || {
            Some((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ))
        }))))
    }

    /// Handles the Tao() RPC request.
    ///
    /// A hash table lookup is performed on a supplied tenant id, table id, and key. If successfull,
    /// the result of the lookup is written into a response packet, and the response header is
    /// updated. In the case of a failure, the response header is updated with a status indicating
    /// the reason for the failure.
    ///
    /// # Arguments
    ///
    /// * `req`: The RPC request packet sent by the client, parsed upto it's UDP header.
    /// * `res`: The RPC response packet, with pre-allocated headers upto UDP.
    ///
    /// # Return
    ///
    /// A Native task that can be scheduled by the database. In the case of an error, the passed
    /// in request and response packets are returned with the response status appropriately set.
    #[allow(unreachable_code)]
    #[allow(unused_assignments)]
    fn tao(
        &self,
        req: Packet<UdpHeader, EmptyMetadata>,
        res: Packet<UdpHeader, EmptyMetadata>,
    ) -> Result<
        Box<Task>,
        (
            Packet<UdpHeader, EmptyMetadata>,
            Packet<UdpHeader, EmptyMetadata>,
        ),
    > {
        // First, parse the request packet.
        let req = req.parse_header::<TaoRequest>();

        // Read fields off the request header.
        let mut tenant_id: TenantId = 0;
        let mut table_id: TableId = 0;
        let mut key_length = 0;
        let mut rpc_stamp = 0;

        {
            let hdr = req.get_header();
            tenant_id = hdr.common_header.tenant as TenantId;
            table_id = hdr.table_id as TableId;
            key_length = hdr.key_length;
            rpc_stamp = hdr.common_header.stamp;
        }

        // Next, add a header to the response packet.
        let mut res = res
            .push_header(&TaoResponse::new(
                rpc_stamp,
                OpCode::SandstormTaoRpc,
                tenant_id,
            ))
            .expect("Failed to setup TaoResponse");

        // If the payload size is less than the key length, return an error.
        if req.get_payload().len() < key_length as usize {
            res.get_mut_header().common_header.status = RpcStatus::StatusMalformedRequest;
            return Err((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ));
        }

        // Lookup the tenant, and get a handle to the allocator. Required to avoid capturing a
        // reference to Master in the generator below.
        let tenant = self.get_tenant(tenant_id);
        let alloc: *const Allocator = &self.heap;
        let mut status: RpcStatus = RpcStatus::StatusTenantDoesNotExist;
        let optype: u8 = 0x3; // OpType::SandstormTao
        let mut res = res;
        let outcome =
            tenant.and_then(|tenant| {
                status = RpcStatus::StatusTableDoesNotExist;
                tenant.get_table(table_id)
            })
            .and_then(|table| {
                status = RpcStatus::StatusObjectDoesNotExist;
                let (key, _) = req.get_payload().split_at(key_length as usize);
                table.get(key)
            })
            .and_then(|entry| {
                status = RpcStatus::StatusInternalError;
                let alloc: &Allocator = accessor(alloc);
                Some((alloc.resolve(entry.value), entry.version))
            })
            .and_then(|(opt, version)| {
                if let Some(opt) = opt {
                    let (k, value) = &opt;
                    status = RpcStatus::StatusInternalError;
                    let _result = res.add_to_payload_tail(1, pack(&optype));
                    let _ = res.add_to_payload_tail(size_of::<Version>(), &unsafe { transmute::<Version, [u8; 8]>(version) });
                    let result = res.add_to_payload_tail(k.len(), &k[..]);
                    match result {
                        Ok(()) => {
                            res.add_to_payload_tail(value.len(), &value[..]).ok()
                        }
                        Err(_) => {
                            Some(())
                        }
                    }
                } else {
                    None
                }
            })
            .and_then(|_| {
                status = RpcStatus::StatusOk;
                Some(())
            });
        match outcome {
            Some(()) => {
                let val_len = res.get_payload().len() as u32;
                let hdr: &mut TaoResponse = res.get_mut_header();
                hdr.value_length = val_len;
                hdr.common_header.status = status;
            }
            None => {
                res.get_mut_header().common_header.status = status;
            }
        }
        Ok(Box::new(Native::new(TaskPriority::REQUEST, Box::pin(move || {
            Some((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ))
        }))))
    }

    /// Handles the Bad() RPC request.
    ///
    /// A hash table lookup is performed on a supplied tenant id, table id, and key. If successfull,
    /// the result of the lookup is written into a response packet, and the response header is
    /// updated. In the case of a failure, the response header is updated with a status indicating
    /// the reason for the failure.
    ///
    /// # Arguments
    ///
    /// * `req`: The RPC request packet sent by the client, parsed upto it's UDP header.
    /// * `res`: The RPC response packet, with pre-allocated headers upto UDP.
    ///
    /// # Return
    ///
    /// A Native task that can be scheduled by the database. In the case of an error, the passed
    /// in request and response packets are returned with the response status appropriately set.
    #[allow(unreachable_code)]
    #[allow(unused_assignments)]
    fn bad(
        &self,
        req: Packet<UdpHeader, EmptyMetadata>,
        res: Packet<UdpHeader, EmptyMetadata>,
    ) -> Result<
        Box<Task>,
        (
            Packet<UdpHeader, EmptyMetadata>,
            Packet<UdpHeader, EmptyMetadata>,
        ),
    > {
        // First, parse the request packet.
        let req = req.parse_header::<BadRequest>();

        // Read fields off the request header.
        let mut tenant_id: TenantId = 0;
        let mut table_id: TableId = 0;
        let mut key_length = 0;
        let mut rpc_stamp = 0;

        {
            let hdr = req.get_header();
            tenant_id = hdr.common_header.tenant as TenantId;
            table_id = hdr.table_id as TableId;
            key_length = hdr.key_length;
            rpc_stamp = hdr.common_header.stamp;
        }

        // Next, add a header to the response packet.
        let mut res = res
            .push_header(&BadResponse::new(
                rpc_stamp,
                OpCode::SandstormBadRpc,
                tenant_id,
            ))
            .expect("Failed to setup BadResponse");

        // If the payload size is less than the key length, return an error.
        if req.get_payload().len() < key_length as usize {
            res.get_mut_header().common_header.status = RpcStatus::StatusMalformedRequest;
            return Err((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ));
        }

        // Lookup the tenant, and get a handle to the allocator. Required to avoid capturing a
        // reference to Master in the generator below.
        let tenant = self.get_tenant(tenant_id);
        let alloc: *const Allocator = &self.heap;
        let mut status: RpcStatus = RpcStatus::StatusTenantDoesNotExist;
        let optype: u8 = 0x4; // OpType::SandstormBad
        let mut res = res;
        let outcome =
            tenant.and_then(|tenant| {
                status = RpcStatus::StatusTableDoesNotExist;
                tenant.get_table(table_id)
            })
            .and_then(|table| {
                status = RpcStatus::StatusObjectDoesNotExist;
                let (key, _) = req.get_payload().split_at(key_length as usize);
                table.get(key)
            })
            .and_then(|entry| {
                status = RpcStatus::StatusInternalError;
                let alloc: &Allocator = accessor(alloc);
                Some((alloc.resolve(entry.value), entry.version))
            })
            .and_then(|(opt, version)| {
                if let Some(opt) = opt {
                    let (k, value) = &opt;
                    status = RpcStatus::StatusInternalError;
                    let _result = res.add_to_payload_tail(1, pack(&optype));
                    let _ = res.add_to_payload_tail(size_of::<Version>(), &unsafe { transmute::<Version, [u8; 8]>(version) });
                    let result = res.add_to_payload_tail(k.len(), &k[..]);
                    match result {
                        Ok(()) => {
                            res.add_to_payload_tail(value.len(), &value[..]).ok()
                        }
                        Err(_) => {
                            Some(())
                        }
                    }
                } else {
                    None
                }
            })
            .and_then(|_| {
                status = RpcStatus::StatusOk;
                Some(())
            });
        match outcome {
            Some(()) => {
                let val_len = res.get_payload().len() as u32;
                let hdr: &mut BadResponse = res.get_mut_header();
                hdr.value_length = val_len;
                hdr.common_header.status = status;
            }
            None => {
                res.get_mut_header().common_header.status = status;
            }
        }
        Ok(Box::new(Native::new(TaskPriority::REQUEST, Box::pin(move || {
            Some((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ))
        }))))
    }

    /// Handles the Long() RPC request.
    ///
    /// A hash table lookup is performed on a supplied tenant id, table id, and key. If successfull,
    /// the result of the lookup is written into a response packet, and the response header is
    /// updated. In the case of a failure, the response header is updated with a status indicating
    /// the reason for the failure.
    ///
    /// # Arguments
    ///
    /// * `req`: The RPC request packet sent by the client, parsed upto it's UDP header.
    /// * `res`: The RPC response packet, with pre-allocated headers upto UDP.
    ///
    /// # Return
    ///
    /// A Native task that can be scheduled by the database. In the case of an error, the passed
    /// in request and response packets are returned with the response status appropriately set.
    #[allow(unreachable_code)]
    #[allow(unused_assignments)]
    fn long(
        &self,
        req: Packet<UdpHeader, EmptyMetadata>,
        res: Packet<UdpHeader, EmptyMetadata>,
    ) -> Result<
        Box<Task>,
        (
            Packet<UdpHeader, EmptyMetadata>,
            Packet<UdpHeader, EmptyMetadata>,
        ),
    > {
        // First, parse the request packet.
        let req = req.parse_header::<LongRequest>();

        // Read fields off the request header.
        let mut tenant_id: TenantId = 0;
        let mut table_id: TableId = 0;
        let mut key_length = 0;
        let mut rpc_stamp = 0;

        {
            let hdr = req.get_header();
            tenant_id = hdr.common_header.tenant as TenantId;
            table_id = hdr.table_id as TableId;
            key_length = hdr.key_length;
            rpc_stamp = hdr.common_header.stamp;
        }

        // Next, add a header to the response packet.
        let mut res = res
            .push_header(&LongResponse::new(
                rpc_stamp,
                OpCode::SandstormLongRpc,
                tenant_id,
            ))
            .expect("Failed to setup LongResponse");

        // If the payload size is less than the key length, return an error.
        if req.get_payload().len() < key_length as usize {
            res.get_mut_header().common_header.status = RpcStatus::StatusMalformedRequest;
            return Err((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ));
        }

        // Lookup the tenant, and get a handle to the allocator. Required to avoid capturing a
        // reference to Master in the generator below.
        let tenant = self.get_tenant(tenant_id);
        let alloc: *const Allocator = &self.heap;
        let mut status: RpcStatus = RpcStatus::StatusTenantDoesNotExist;
        let optype: u8 = 0x5; // OpType::SandstormLong
        let mut res = res;
        let outcome =
            tenant.and_then(|tenant| {
                status = RpcStatus::StatusTableDoesNotExist;
                tenant.get_table(table_id)
            })
            .and_then(|table| {
                status = RpcStatus::StatusObjectDoesNotExist;
                let (key, _) = req.get_payload().split_at(key_length as usize);
                table.get(key)
            })
            .and_then(|entry| {
                status = RpcStatus::StatusInternalError;
                let alloc: &Allocator = accessor(alloc);
                Some((alloc.resolve(entry.value), entry.version))
            })
            .and_then(|(opt, version)| {
                if let Some(opt) = opt {
                    let (k, value) = &opt;
                    status = RpcStatus::StatusInternalError;
                    let _result = res.add_to_payload_tail(1, pack(&optype));
                    let _ = res.add_to_payload_tail(size_of::<Version>(), &unsafe { transmute::<Version, [u8; 8]>(version) });
                    let result = res.add_to_payload_tail(k.len(), &k[..]);
                    match result {
                        Ok(()) => {
                            res.add_to_payload_tail(value.len(), &value[..]).ok()
                        }
                        Err(_) => {
                            Some(())
                        }
                    }
                } else {
                    None
                }
            })
            .and_then(|_| {
                status = RpcStatus::StatusOk;
                Some(())
            });
        match outcome {
            Some(()) => {
                let val_len = res.get_payload().len() as u32;
                let hdr: &mut LongResponse = res.get_mut_header();
                hdr.value_length = val_len;
                hdr.common_header.status = status;
            }
            None => {
                res.get_mut_header().common_header.status = status;
            }
        }
        Ok(Box::new(Native::new(TaskPriority::REQUEST, Box::pin(move || {
            Some((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ))
        }))))
    }

    /// Handles the Aggregate() RPC request.
    ///
    /// A hash table lookup is performed on a supplied tenant id, table id, and key. If successfull,
    /// the result of the lookup is written into a response packet, and the response header is
    /// updated. In the case of a failure, the response header is updated with a status indicating
    /// the reason for the failure.
    ///
    /// # Arguments
    ///
    /// * `req`: The RPC request packet sent by the client, parsed upto it's UDP header.
    /// * `res`: The RPC response packet, with pre-allocated headers upto UDP.
    ///
    /// # Return
    ///
    /// A Native task that can be scheduled by the database. In the case of an error, the passed
    /// in request and response packets are returned with the response status appropriately set.
    #[allow(unreachable_code)]
    #[allow(unused_assignments)]
    fn aggregate(
        &self,
        req: Packet<UdpHeader, EmptyMetadata>,
        res: Packet<UdpHeader, EmptyMetadata>,
    ) -> Result<
        Box<Task>,
        (
            Packet<UdpHeader, EmptyMetadata>,
            Packet<UdpHeader, EmptyMetadata>,
        ),
    > {
        // First, parse the request packet.
        let req = req.parse_header::<AggregateRequest>();

        // Read fields off the request header.
        let mut tenant_id: TenantId = 0;
        let mut table_id: TableId = 0;
        let mut key_length = 0;
        let mut rpc_stamp = 0;

        {
            let hdr = req.get_header();
            tenant_id = hdr.common_header.tenant as TenantId;
            table_id = hdr.table_id as TableId;
            key_length = hdr.key_length;
            rpc_stamp = hdr.common_header.stamp;
        }

        // Next, add a header to the response packet.
        let mut res = res
            .push_header(&AggregateResponse::new(
                rpc_stamp,
                OpCode::SandstormAggregateRpc,
                tenant_id,
            ))
            .expect("Failed to setup AggregateResponse");

        // If the payload size is less than the key length, return an error.
        if req.get_payload().len() < key_length as usize {
            res.get_mut_header().common_header.status = RpcStatus::StatusMalformedRequest;
            return Err((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ));
        }

        // Lookup the tenant, and get a handle to the allocator. Required to avoid capturing a
        // reference to Master in the generator below.
        let tenant = self.get_tenant(tenant_id);
        let alloc: *const Allocator = &self.heap;
        let mut status: RpcStatus = RpcStatus::StatusTenantDoesNotExist;
        let optype: u8 = 0x6; // OpType::SandstormAggregate
        let mut res = res;
        let outcome =
            tenant.and_then(|tenant| {
                status = RpcStatus::StatusTableDoesNotExist;
                tenant.get_table(table_id)
            })
            .and_then(|table| {
                status = RpcStatus::StatusObjectDoesNotExist;
                let (key, _) = req.get_payload().split_at(key_length as usize);
                table.get(key)
            })
            .and_then(|entry| {
                status = RpcStatus::StatusInternalError;
                let alloc: &Allocator = accessor(alloc);
                Some((alloc.resolve(entry.value), entry.version))
            })
            .and_then(|(opt, version)| {
                if let Some(opt) = opt {
                    let (k, value) = &opt;
                    status = RpcStatus::StatusInternalError;
                    let _result = res.add_to_payload_tail(1, pack(&optype));
                    let _ = res.add_to_payload_tail(size_of::<Version>(), &unsafe { transmute::<Version, [u8; 8]>(version) });
                    let result = res.add_to_payload_tail(k.len(), &k[..]);
                    match result {
                        Ok(()) => {
                            res.add_to_payload_tail(value.len(), &value[..]).ok()
                        }
                        Err(_) => {
                            Some(())
                        }
                    }
                } else {
                    None
                }
            })
            .and_then(|_| {
                status = RpcStatus::StatusOk;
                Some(())
            });
        match outcome {
            Some(()) => {
                let val_len = res.get_payload().len() as u32;
                let hdr: &mut AggregateResponse = res.get_mut_header();
                hdr.value_length = val_len;
                hdr.common_header.status = status;
            }
            None => {
                res.get_mut_header().common_header.status = status;
            }
        }
        Ok(Box::new(Native::new(TaskPriority::REQUEST, Box::pin(move || {
            Some((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ))
        }))))
    }

    /// Handles the Pushback() RPC request.
    ///
    /// A hash table lookup is performed on a supplied tenant id, table id, and key. If successfull,
    /// the result of the lookup is written into a response packet, and the response header is
    /// updated. In the case of a failure, the response header is updated with a status indicating
    /// the reason for the failure.
    ///
    /// # Arguments
    ///
    /// * `req`: The RPC request packet sent by the client, parsed upto it's UDP header.
    /// * `res`: The RPC response packet, with pre-allocated headers upto UDP.
    ///
    /// # Return
    ///
    /// A Native task that can be scheduled by the database. In the case of an error, the passed
    /// in request and response packets are returned with the response status appropriately set.
    #[allow(unreachable_code)]
    #[allow(unused_assignments)]
    fn pushback(
        &self,
        req: Packet<UdpHeader, EmptyMetadata>,
        res: Packet<UdpHeader, EmptyMetadata>,
    ) -> Result<
        Box<Task>,
        (
            Packet<UdpHeader, EmptyMetadata>,
            Packet<UdpHeader, EmptyMetadata>,
        ),
    > {
        // First, parse the request packet.
        let req = req.parse_header::<PushbackRequest>();

        // Read fields off the request header.
        let mut tenant_id: TenantId = 0;
        let mut table_id: TableId = 0;
        let mut key_length = 0;
        let mut rpc_stamp = 0;

        {
            let hdr = req.get_header();
            tenant_id = hdr.common_header.tenant as TenantId;
            table_id = hdr.table_id as TableId;
            key_length = hdr.key_length;
            rpc_stamp = hdr.common_header.stamp;
        }

        // Next, add a header to the response packet.
        let mut res = res
            .push_header(&PushbackResponse::new(
                rpc_stamp,
                OpCode::SandstormPushbackRpc,
                tenant_id,
            ))
            .expect("Failed to setup PushbackResponse");

        // If the payload size is less than the key length, return an error.
        if req.get_payload().len() < key_length as usize {
            res.get_mut_header().common_header.status = RpcStatus::StatusMalformedRequest;
            return Err((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ));
        }

        // Lookup the tenant, and get a handle to the allocator. Required to avoid capturing a
        // reference to Master in the generator below.
        let tenant = self.get_tenant(tenant_id);
        let alloc: *const Allocator = &self.heap;
        let mut status: RpcStatus = RpcStatus::StatusTenantDoesNotExist;
        let optype: u8 = 0x7; // OpType::SandstormPushback
        let mut res = res;
        let outcome =
            tenant.and_then(|tenant| {
                status = RpcStatus::StatusTableDoesNotExist;
                tenant.get_table(table_id)
            })
            .and_then(|table| {
                status = RpcStatus::StatusObjectDoesNotExist;
                let (key, _) = req.get_payload().split_at(key_length as usize);
                table.get(key)
            })
            .and_then(|entry| {
                status = RpcStatus::StatusInternalError;
                let alloc: &Allocator = accessor(alloc);
                Some((alloc.resolve(entry.value), entry.version))
            })
            .and_then(|(opt, version)| {
                if let Some(opt) = opt {
                    let (k, value) = &opt;
                    status = RpcStatus::StatusInternalError;
                    let _result = res.add_to_payload_tail(1, pack(&optype));
                    let _ = res.add_to_payload_tail(size_of::<Version>(), &unsafe { transmute::<Version, [u8; 8]>(version) });
                    let result = res.add_to_payload_tail(k.len(), &k[..]);
                    match result {
                        Ok(()) => {
                            res.add_to_payload_tail(value.len(), &value[..]).ok()
                        }
                        Err(_) => {
                            Some(())
                        }
                    }
                } else {
                    None
                }
            })
            .and_then(|_| {
                status = RpcStatus::StatusOk;
                Some(())
            });
        match outcome {
            Some(()) => {
                let val_len = res.get_payload().len() as u32;
                let hdr: &mut PushbackResponse = res.get_mut_header();
                hdr.value_length = val_len;
                hdr.common_header.status = status;
            }
            None => {
                res.get_mut_header().common_header.status = status;
            }
        }
        Ok(Box::new(Native::new(TaskPriority::REQUEST, Box::pin(move || {
            Some((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ))
        }))))
    }

    /// Handles the Scan() RPC request.
    ///
    /// A hash table lookup is performed on a supplied tenant id, table id, and key. If successfull,
    /// the result of the lookup is written into a response packet, and the response header is
    /// updated. In the case of a failure, the response header is updated with a status indicating
    /// the reason for the failure.
    ///
    /// # Arguments
    ///
    /// * `req`: The RPC request packet sent by the client, parsed upto it's UDP header.
    /// * `res`: The RPC response packet, with pre-allocated headers upto UDP.
    ///
    /// # Return
    ///
    /// A Native task that can be scheduled by the database. In the case of an error, the passed
    /// in request and response packets are returned with the response status appropriately set.
    #[allow(unreachable_code)]
    #[allow(unused_assignments)]
    fn scan(
        &self,
        req: Packet<UdpHeader, EmptyMetadata>,
        res: Packet<UdpHeader, EmptyMetadata>,
    ) -> Result<
        Box<Task>,
        (
            Packet<UdpHeader, EmptyMetadata>,
            Packet<UdpHeader, EmptyMetadata>,
        ),
    > {
        // First, parse the request packet.
        let req = req.parse_header::<ScanRequest>();

        // Read fields off the request header.
        let mut tenant_id: TenantId = 0;
        let mut table_id: TableId = 0;
        let mut key_length = 0;
        let mut rpc_stamp = 0;

        {
            let hdr = req.get_header();
            tenant_id = hdr.common_header.tenant as TenantId;
            table_id = hdr.table_id as TableId;
            key_length = hdr.key_length;
            rpc_stamp = hdr.common_header.stamp;
        }

        // Next, add a header to the response packet.
        let mut res = res
            .push_header(&ScanResponse::new(
                rpc_stamp,
                OpCode::SandstormScanRpc,
                tenant_id,
            ))
            .expect("Failed to setup ScanResponse");

        // If the payload size is less than the key length, return an error.
        if req.get_payload().len() < key_length as usize {
            res.get_mut_header().common_header.status = RpcStatus::StatusMalformedRequest;
            return Err((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ));
        }

        // Lookup the tenant, and get a handle to the allocator. Required to avoid capturing a
        // reference to Master in the generator below.
        let tenant = self.get_tenant(tenant_id);
        let alloc: *const Allocator = &self.heap;
        let mut status: RpcStatus = RpcStatus::StatusTenantDoesNotExist;
        let optype: u8 = 0x8; // OpType::SandstormScan
        let mut res = res;
        let outcome =
            tenant.and_then(|tenant| {
                status = RpcStatus::StatusTableDoesNotExist;
                tenant.get_table(table_id)
            })
            .and_then(|table| {
                status = RpcStatus::StatusObjectDoesNotExist;
                let (key, _) = req.get_payload().split_at(key_length as usize);
                table.get(key)
            })
            .and_then(|entry| {
                status = RpcStatus::StatusInternalError;
                let alloc: &Allocator = accessor(alloc);
                Some((alloc.resolve(entry.value), entry.version))
            })
            .and_then(|(opt, version)| {
                if let Some(opt) = opt {
                    let (k, value) = &opt;
                    status = RpcStatus::StatusInternalError;
                    let _result = res.add_to_payload_tail(1, pack(&optype));
                    let _ = res.add_to_payload_tail(size_of::<Version>(), &unsafe { transmute::<Version, [u8; 8]>(version) });
                    let result = res.add_to_payload_tail(k.len(), &k[..]);
                    match result {
                        Ok(()) => {
                            res.add_to_payload_tail(value.len(), &value[..]).ok()
                        }
                        Err(_) => {
                            Some(())
                        }
                    }
                } else {
                    None
                }
            })
            .and_then(|_| {
                status = RpcStatus::StatusOk;
                Some(())
            });
        match outcome {
            Some(()) => {
                let val_len = res.get_payload().len() as u32;
                let hdr: &mut ScanResponse = res.get_mut_header();
                hdr.value_length = val_len;
                hdr.common_header.status = status;
            }
            None => {
                res.get_mut_header().common_header.status = status;
            }
        }
        Ok(Box::new(Native::new(TaskPriority::REQUEST, Box::pin(move || {
            Some((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ))
        }))))
    }

    /// Handles the Analysis() RPC request.
    ///
    /// A hash table lookup is performed on a supplied tenant id, table id, and key. If successfull,
    /// the result of the lookup is written into a response packet, and the response header is
    /// updated. In the case of a failure, the response header is updated with a status indicating
    /// the reason for the failure.
    ///
    /// # Arguments
    ///
    /// * `req`: The RPC request packet sent by the client, parsed upto it's UDP header.
    /// * `res`: The RPC response packet, with pre-allocated headers upto UDP.
    ///
    /// # Return
    ///
    /// A Native task that can be scheduled by the database. In the case of an error, the passed
    /// in request and response packets are returned with the response status appropriately set.
    #[allow(unreachable_code)]
    #[allow(unused_assignments)]
    fn analysis(
        &self,
        req: Packet<UdpHeader, EmptyMetadata>,
        res: Packet<UdpHeader, EmptyMetadata>,
    ) -> Result<
        Box<Task>,
        (
            Packet<UdpHeader, EmptyMetadata>,
            Packet<UdpHeader, EmptyMetadata>,
        ),
    > {
        // First, parse the request packet.
        let req = req.parse_header::<AnalysisRequest>();

        // Read fields off the request header.
        let mut tenant_id: TenantId = 0;
        let mut table_id: TableId = 0;
        let mut key_length = 0;
        let mut rpc_stamp = 0;

        {
            let hdr = req.get_header();
            tenant_id = hdr.common_header.tenant as TenantId;
            table_id = hdr.table_id as TableId;
            key_length = hdr.key_length;
            rpc_stamp = hdr.common_header.stamp;
        }

        // Next, add a header to the response packet.
        let mut res = res
            .push_header(&AnalysisResponse::new(
                rpc_stamp,
                OpCode::SandstormAnalysisRpc,
                tenant_id,
            ))
            .expect("Failed to setup AnalysisResponse");

        // If the payload size is less than the key length, return an error.
        if req.get_payload().len() < key_length as usize {
            res.get_mut_header().common_header.status = RpcStatus::StatusMalformedRequest;
            return Err((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ));
        }

        // Lookup the tenant, and get a handle to the allocator. Required to avoid capturing a
        // reference to Master in the generator below.
        let tenant = self.get_tenant(tenant_id);
        let alloc: *const Allocator = &self.heap;
        let mut status: RpcStatus = RpcStatus::StatusTenantDoesNotExist;
        let optype: u8 = 0x9; // OpType::SandstormAnalysis
        let mut res = res;
        let outcome =
            tenant.and_then(|tenant| {
                status = RpcStatus::StatusTableDoesNotExist;
                tenant.get_table(table_id)
            })
            .and_then(|table| {
                status = RpcStatus::StatusObjectDoesNotExist;
                let (key, _) = req.get_payload().split_at(key_length as usize);
                table.get(key)
            })
            .and_then(|entry| {
                status = RpcStatus::StatusInternalError;
                let alloc: &Allocator = accessor(alloc);
                Some((alloc.resolve(entry.value), entry.version))
            })
            .and_then(|(opt, version)| {
                if let Some(opt) = opt {
                    let (k, value) = &opt;
                    status = RpcStatus::StatusInternalError;
                    let _result = res.add_to_payload_tail(1, pack(&optype));
                    let _ = res.add_to_payload_tail(size_of::<Version>(), &unsafe { transmute::<Version, [u8; 8]>(version) });
                    let result = res.add_to_payload_tail(k.len(), &k[..]);
                    match result {
                        Ok(()) => {
                            res.add_to_payload_tail(value.len(), &value[..]).ok()
                        }
                        Err(_) => {
                            Some(())
                        }
                    }
                } else {
                    None
                }
            })
            .and_then(|_| {
                status = RpcStatus::StatusOk;
                Some(())
            });
        match outcome {
            Some(()) => {
                let val_len = res.get_payload().len() as u32;
                let hdr: &mut AnalysisResponse = res.get_mut_header();
                hdr.value_length = val_len;
                hdr.common_header.status = status;
            }
            None => {
                res.get_mut_header().common_header.status = status;
            }
        }
        Ok(Box::new(Native::new(TaskPriority::REQUEST, Box::pin(move || {
            Some((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ))
        }))))
    }

    /// Handles the Auth() RPC request.
    ///
    /// A hash table lookup is performed on a supplied tenant id, table id, and key. If successfull,
    /// the result of the lookup is written into a response packet, and the response header is
    /// updated. In the case of a failure, the response header is updated with a status indicating
    /// the reason for the failure.
    ///
    /// # Arguments
    ///
    /// * `req`: The RPC request packet sent by the client, parsed upto it's UDP header.
    /// * `res`: The RPC response packet, with pre-allocated headers upto UDP.
    ///
    /// # Return
    ///
    /// A Native task that can be scheduled by the database. In the case of an error, the passed
    /// in request and response packets are returned with the response status appropriately set.
    #[allow(unreachable_code)]
    #[allow(unused_assignments)]
    fn auth(
        &self,
        req: Packet<UdpHeader, EmptyMetadata>,
        res: Packet<UdpHeader, EmptyMetadata>,
    ) -> Result<
        Box<Task>,
        (
            Packet<UdpHeader, EmptyMetadata>,
                       Packet<UdpHeader, EmptyMetadata>,
        ),
    > {
        // First, parse the request packet.
        let req = req.parse_header::<AuthRequest>();

        // Read fields off the request header.
        let mut tenant_id: TenantId = 0;
        let mut table_id: TableId = 0;
        let mut key_length = 0;
        let mut rpc_stamp = 0;

        {
            let hdr = req.get_header();
            tenant_id = hdr.common_header.tenant as TenantId;
            table_id = hdr.table_id as TableId;
            key_length = hdr.key_length;
            rpc_stamp = hdr.common_header.stamp;
        }

        // Next, add a header to the response packet.
        let mut res = res
            .push_header(&AuthResponse::new(
                rpc_stamp,
                OpCode::SandstormAuthRpc,
                tenant_id,
            ))
            .expect("Failed to setup AuthResponse");

        // If the payload size is less than the key length, return an error.
        if req.get_payload().len() < key_length as usize {
            res.get_mut_header().common_header.status = RpcStatus::StatusMalformedRequest;
            return Err((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ));
        }

        // Lookup the tenant, and get a handle to the allocator. Required to avoid capturing a
        // reference to Master in the generator below.
        let tenant = self.get_tenant(tenant_id);
        let alloc: *const Allocator = &self.heap;
        let mut status: RpcStatus = RpcStatus::StatusTenantDoesNotExist;
        let optype: u8 = 0xa; // OpType::SandstormAuth
        let mut res = res;
        let outcome =
            tenant.and_then(|tenant| {
                status = RpcStatus::StatusTableDoesNotExist;
                tenant.get_table(table_id)
            })
            .and_then(|table| {
                status = RpcStatus::StatusObjectDoesNotExist;
                let (key, _) = req.get_payload().split_at(key_length as usize);
                table.get(key)
            })
            .and_then(|entry| {
                status = RpcStatus::StatusInternalError;
                let alloc: &Allocator = accessor(alloc);
                Some((alloc.resolve(entry.value), entry.version))
            })
            .and_then(|(opt, version)| {
                if let Some(opt) = opt {
                    let (k, value) = &opt;
                    status = RpcStatus::StatusInternalError;
                    let _result = res.add_to_payload_tail(1, pack(&optype));
                    let _ = res.add_to_payload_tail(size_of::<Version>(), &unsafe { transmute::<Version, [u8; 8]>(version) });
                    let result = res.add_to_payload_tail(k.len(), &k[..]);
                    match result {
                        Ok(()) => {
                            res.add_to_payload_tail(value.len(), &value[..]).ok()
                        }
                        Err(_) => {
                            Some(())
                        }
                    }
                } else {
                    None
                }
            })
            .and_then(|_| {
                status = RpcStatus::StatusOk;
                Some(())
            });
        match outcome {
            Some(()) => {
                let val_len = res.get_payload().len() as u32;
                let hdr: &mut AuthResponse = res.get_mut_header();
                hdr.value_length = val_len;
                hdr.common_header.status = status;
            }
            None => {
                res.get_mut_header().common_header.status = status;
            }
        }
        Ok(Box::new(Native::new(TaskPriority::REQUEST, Box::pin(move || {
            Some((
                req.deparse_header(PACKET_UDP_LEN as usize),
                res.deparse_header(PACKET_UDP_LEN as usize),
            ))
        }))))
    }
}