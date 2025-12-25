import os
import unittest

from .helpers.ptrack_helpers import ProbackupException, ProbackupTest


class CompressionTest(ProbackupTest, unittest.TestCase):
    # @unittest.skip("skip")
    # @unittest.expectedFailure
    def test_basic_compression_stream_zlib(self):
        """
        make archive node, make full and page stream backups,
        check data correctness in restored instance
        """
        self.maxDiff = None
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, "backup")
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, "node"),
            set_replication=True,
            initdb_params=["--data-checksums"],
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,256) i",
        )
        full_result = node.table_checksum("t_heap")
        full_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="full",
            options=["--stream", "--compress-algorithm=zlib"],
        )

        # PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(256,512) i",
        )
        page_result = node.table_checksum("t_heap")
        page_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="page",
            options=["--stream", "--compress-algorithm=zlib"],
        )

        # DELTA BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(512,768) i",
        )
        delta_result = node.table_checksum("t_heap")
        delta_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="delta",
            options=["--stream", "--compress-algorithm=zlib"],
        )

        # Drop Node
        node.cleanup()

        # Check full backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=full_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        full_result_new = node.table_checksum("t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check page backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(page_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=page_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        page_result_new = node.table_checksum("t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

        # Check delta backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(delta_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=delta_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        delta_result_new = node.table_checksum("t_heap")
        self.assertEqual(delta_result, delta_result_new)

    def test_compression_archive_zlib(self):
        """
        make archive node, make full and page backups,
        check data correctness in restored instance
        """
        self.maxDiff = None
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, "backup")
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, "node"),
            set_replication=True,
            initdb_params=["--data-checksums"],
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,1) i",
        )
        full_result = node.table_checksum("t_heap")
        full_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="full",
            options=["--compress-algorithm=zlib"],
        )

        # PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(0,2) i",
        )
        page_result = node.table_checksum("t_heap")
        page_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="page",
            options=["--compress-algorithm=zlib"],
        )

        # DELTA BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,3) i",
        )
        delta_result = node.table_checksum("t_heap")
        delta_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="delta",
            options=["--compress-algorithm=zlib"],
        )

        # Drop Node
        node.cleanup()

        # Check full backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=full_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        full_result_new = node.table_checksum("t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check page backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(page_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=page_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        page_result_new = node.table_checksum("t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

        # Check delta backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(delta_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=delta_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        delta_result_new = node.table_checksum("t_heap")
        self.assertEqual(delta_result, delta_result_new)
        node.cleanup()

    def test_compression_stream_pglz(self):
        """
        make archive node, make full and page stream backups,
        check data correctness in restored instance
        """
        self.maxDiff = None
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, "backup")
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, "node"),
            set_replication=True,
            initdb_params=["--data-checksums"],
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,256) i",
        )
        full_result = node.table_checksum("t_heap")
        full_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="full",
            options=["--stream", "--compress-algorithm=pglz"],
        )

        # PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(256,512) i",
        )
        page_result = node.table_checksum("t_heap")
        page_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="page",
            options=["--stream", "--compress-algorithm=pglz"],
        )

        # DELTA BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(512,768) i",
        )
        delta_result = node.table_checksum("t_heap")
        delta_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="delta",
            options=["--stream", "--compress-algorithm=pglz"],
        )

        # Drop Node
        node.cleanup()

        # Check full backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=full_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        full_result_new = node.table_checksum("t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check page backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(page_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=page_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        page_result_new = node.table_checksum("t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

        # Check delta backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(delta_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=delta_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        delta_result_new = node.table_checksum("t_heap")
        self.assertEqual(delta_result, delta_result_new)
        node.cleanup()

    def test_compression_archive_pglz(self):
        """
        make archive node, make full and page backups,
        check data correctness in restored instance
        """
        self.maxDiff = None
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, "backup")
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, "node"),
            set_replication=True,
            initdb_params=["--data-checksums"],
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(0,100) i",
        )
        full_result = node.table_checksum("t_heap")
        full_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="full",
            options=["--compress-algorithm=pglz"],
        )

        # PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(100,200) i",
        )
        page_result = node.table_checksum("t_heap")
        page_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="page",
            options=["--compress-algorithm=pglz"],
        )

        # DELTA BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(200,300) i",
        )
        delta_result = node.table_checksum("t_heap")
        delta_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="delta",
            options=["--compress-algorithm=pglz"],
        )

        # Drop Node
        node.cleanup()

        # Check full backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=full_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        full_result_new = node.table_checksum("t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check page backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(page_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=page_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        page_result_new = node.table_checksum("t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

        # Check delta backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(delta_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=delta_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        delta_result_new = node.table_checksum("t_heap")
        self.assertEqual(delta_result, delta_result_new)
        node.cleanup()

    def test_compression_wrong_algorithm(self):
        """
        make archive node, make full and page backups,
        check data correctness in restored instance
        """
        self.maxDiff = None
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, "backup")
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, "node"),
            set_replication=True,
            initdb_params=["--data-checksums"],
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        try:
            self.backup_node(
                backup_dir,
                "node",
                node,
                backup_type="full",
                options=["--compress-algorithm=bla-blah"],
            )
            # we should die here because exception is what we expect to happen
            self.assertEqual(
                1,
                0,
                "Expecting Error because compress-algorithm is invalid.\n Output: {0} \n CMD: {1}".format(
                    repr(self.output), self.cmd
                ),
            )
        except ProbackupException as e:
            self.assertEqual(
                e.message,
                'ERROR: Invalid compress algorithm value "bla-blah"\n',
                "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(e.message), self.cmd),
            )

    # @unittest.skip("skip")
    def test_incompressible_pages(self):
        """
        make archive node, create table with incompressible toast pages,
        take backup with compression, make sure that page was not compressed,
        restore backup and check data correctness
        """
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, "backup")
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, "node"),
            set_replication=True,
            initdb_params=["--data-checksums"],
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # Full
        self.backup_node(
            backup_dir,
            "node",
            node,
            options=["--compress-algorithm=zlib", "--compress-level=0"],
        )

        node.pgbench_init(scale=3)

        self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="delta",
            options=["--compress-algorithm=zlib", "--compress-level=0"],
        )

        pgdata = self.pgdata_content(node.data_dir)

        node.cleanup()

        self.restore_node(backup_dir, "node", node)

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

        node.slow_start()

    def test_basic_compression_stream_lz4(self):
        """
        make archive node, make full, page and delta stream backups with LZ4,
        check data correctness in restored instance
        """
        self.maxDiff = None
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, "backup")
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, "node"),
            set_replication=True,
            initdb_params=["--data-checksums"],
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,256) i",
        )
        full_result = node.table_checksum("t_heap")
        full_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="full",
            options=["--stream", "--compress-algorithm=lz4"],
        )

        # PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(256,512) i",
        )
        page_result = node.table_checksum("t_heap")
        page_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="page",
            options=["--stream", "--compress-algorithm=lz4"],
        )

        # DELTA BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(512,768) i",
        )
        delta_result = node.table_checksum("t_heap")
        delta_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="delta",
            options=["--stream", "--compress-algorithm=lz4"],
        )

        # Drop Node
        node.cleanup()

        # Check full backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=full_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        full_result_new = node.table_checksum("t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check page backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(page_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=page_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        page_result_new = node.table_checksum("t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

        # Check delta backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(delta_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=delta_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        delta_result_new = node.table_checksum("t_heap")
        self.assertEqual(delta_result, delta_result_new)

    def test_compression_archive_lz4(self):
        """
        make archive node, make full, page and delta backups with LZ4,
        check data correctness in restored instance
        """
        self.maxDiff = None
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, "backup")
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, "node"),
            set_replication=True,
            initdb_params=["--data-checksums"],
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,1) i",
        )
        full_result = node.table_checksum("t_heap")
        full_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="full",
            options=["--compress-algorithm=lz4"],
        )

        # PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(0,2) i",
        )
        page_result = node.table_checksum("t_heap")
        page_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="page",
            options=["--compress-algorithm=lz4"],
        )

        # DELTA BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,3) i",
        )
        delta_result = node.table_checksum("t_heap")
        delta_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="delta",
            options=["--compress-algorithm=lz4"],
        )

        # Drop Node
        node.cleanup()

        # Check full backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=full_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        full_result_new = node.table_checksum("t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check page backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(page_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=page_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        page_result_new = node.table_checksum("t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

        # Check delta backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(delta_backup_id),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=delta_backup_id,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        delta_result_new = node.table_checksum("t_heap")
        self.assertEqual(delta_result, delta_result_new)
        node.cleanup()

    def test_lz4_compression_levels(self):
        """
        test LZ4 compression with different levels (0=fast, 1-12=HC)
        """
        self.maxDiff = None
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, "backup")
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, "node"),
            set_replication=True,
            initdb_params=["--data-checksums"],
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # Create test data
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000) i",
        )

        # Test level 0 (fast compression)
        self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="full",
            options=["--stream", "--compress-algorithm=lz4", "--compress-level=0"],
        )

        # Test level 6 (medium HC compression)
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(1000,2000) i",
        )
        self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="delta",
            options=["--stream", "--compress-algorithm=lz4", "--compress-level=6"],
        )

        # Test level 12 (max HC compression)
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(2000,3000) i",
        )
        result_level12 = node.table_checksum("t_heap")
        backup_id_level12 = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="delta",
            options=["--stream", "--compress-algorithm=lz4", "--compress-level=12"],
        )

        # Drop Node
        node.cleanup()

        # Check level 12 backup restore
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(backup_id_level12),
            self.restore_node(
                backup_dir,
                "node",
                node,
                backup_id=backup_id_level12,
                options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
            ),
            "\n Unexpected Error Message: {0}\n CMD: {1}".format(repr(self.output), self.cmd),
        )
        node.slow_start()

        restored_result = node.table_checksum("t_heap")
        self.assertEqual(result_level12, restored_result)

    def test_merge_lz4_backups(self):
        """
        Test MERGE command with LZ4 compressed backups:
        FULL(lz4) -> PAGE(lz4) -> DELTA(lz4) -> MERGE
        """
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, "backup")
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, "node"),
            set_replication=True,
            initdb_params=["--data-checksums"],
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # FULL backup with LZ4
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000) i",
        )

        self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="full",
            options=["--stream", "--compress-algorithm=lz4"],
        )

        # PAGE backup with LZ4
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(1000,2000) i",
        )

        self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="page",
            options=["--stream", "--compress-algorithm=lz4"],
        )

        # DELTA backup with LZ4
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(2000,3000) i",
        )

        final_result = node.table_checksum("t_heap")
        delta_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="delta",
            options=["--stream", "--compress-algorithm=lz4"],
        )

        # Save pgdata for comparison
        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # Merge all backups
        self.merge_backup(backup_dir, "node", delta_backup_id, options=["-j", "4"])

        # Check merge result
        show_backups = self.show_pb(backup_dir, "node")
        self.assertEqual(len(show_backups), 1)
        self.assertEqual(show_backups[0]["status"], "OK")
        self.assertEqual(show_backups[0]["backup-mode"], "FULL")

        # Restore and verify
        node.cleanup()
        self.restore_node(
            backup_dir,
            "node",
            node,
            options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
        )
        node.slow_start()

        restored_result = node.table_checksum("t_heap")
        self.assertEqual(final_result, restored_result)

        # Physical comparison
        if self.paranoia:
            pgdata_restored = self.pgdata_content(node.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

    def test_validate_lz4_backup(self):
        """
        Test VALIDATE command with LZ4 compressed backups
        """
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, "backup")
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, "node"),
            set_replication=True,
            initdb_params=["--data-checksums"],
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # Create data and make FULL backup with LZ4
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text from generate_series(0,10000) i",
        )

        full_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="full",
            options=["--stream", "--compress-algorithm=lz4"],
        )

        # Validate FULL backup
        self.validate_pb(backup_dir, "node", full_backup_id, options=["-j", "4"])

        # Make incremental backup
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text from generate_series(10000,20000) i",
        )

        delta_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="delta",
            options=["--stream", "--compress-algorithm=lz4"],
        )

        # Validate DELTA backup
        self.validate_pb(backup_dir, "node", delta_backup_id, options=["-j", "4"])

        # Validate entire instance
        self.validate_pb(backup_dir, "node", options=["-j", "4"])

        # Check backup statuses
        full_status = self.show_pb(backup_dir, "node", full_backup_id)["status"]
        delta_status = self.show_pb(backup_dir, "node", delta_backup_id)["status"]

        self.assertEqual(full_status, "OK")
        self.assertEqual(delta_status, "OK")

    def test_mixed_compression_chain_zlib_lz4(self):
        """
        Test backup chain with mixed compression algorithms:
        FULL(zlib) -> PAGE(lz4) -> DELTA(lz4) -> restore
        """
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, "backup")
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, "node"),
            set_replication=True,
            initdb_params=["--data-checksums"],
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # FULL backup with ZLIB
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,500) i",
        )

        self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="full",
            options=["--stream", "--compress-algorithm=zlib"],
        )

        # PAGE backup with LZ4
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(500,1000) i",
        )
        page_result = node.table_checksum("t_heap")

        page_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="page",
            options=["--stream", "--compress-algorithm=lz4"],
        )

        # DELTA backup with LZ4
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(1000,1500) i",
        )
        delta_result = node.table_checksum("t_heap")

        delta_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="delta",
            options=["--stream", "--compress-algorithm=lz4"],
        )

        # Validate the mixed chain
        self.validate_pb(backup_dir, "node", options=["-j", "4"])

        # Restore from DELTA (should use entire chain)
        node.cleanup()
        self.restore_node(
            backup_dir,
            "node",
            node,
            backup_id=delta_backup_id,
            options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
        )
        node.slow_start()

        restored_result = node.table_checksum("t_heap")
        self.assertEqual(delta_result, restored_result)

        # Restore from PAGE backup
        node.cleanup()
        self.restore_node(
            backup_dir,
            "node",
            node,
            backup_id=page_backup_id,
            options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
        )
        node.slow_start()

        restored_result = node.table_checksum("t_heap")
        self.assertEqual(page_result, restored_result)

    def test_merge_mixed_compression_zlib_lz4(self):
        """
        Test MERGE with mixed compression: FULL(zlib) + DELTA(lz4) -> MERGE
        """
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, "backup")
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, "node"),
            set_replication=True,
            initdb_params=["--data-checksums"],
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # FULL backup with ZLIB
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text from generate_series(0,1000) i",
        )

        self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="full",
            options=["--stream", "--compress-algorithm=zlib"],
        )

        # DELTA backup with LZ4
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text from generate_series(1000,2000) i",
        )

        node.safe_psql("postgres", "delete from t_heap where id < 500")

        node.safe_psql("postgres", "vacuum t_heap")

        final_result = node.table_checksum("t_heap")

        delta_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="delta",
            options=["--stream", "--compress-algorithm=lz4"],
        )

        if self.paranoia:
            pgdata = self.pgdata_content(node.data_dir)

        # Merge
        self.merge_backup(backup_dir, "node", delta_backup_id, options=["-j", "4"])

        # Check merge result
        show_backups = self.show_pb(backup_dir, "node")
        self.assertEqual(len(show_backups), 1)
        self.assertEqual(show_backups[0]["status"], "OK")

        # Restore and verify
        node.cleanup()
        self.restore_node(
            backup_dir,
            "node",
            node,
            options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
        )
        node.slow_start()

        restored_result = node.table_checksum("t_heap")
        self.assertEqual(final_result, restored_result)

        if self.paranoia:
            pgdata_restored = self.pgdata_content(node.data_dir)
            self.compare_pgdata(pgdata, pgdata_restored)

    def test_lz4_incompressible_data(self):
        """
        Test LZ4 compression with incompressible (random) data.
        When compression doesn't help, pages should be stored uncompressed.
        """
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, "backup")
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, "node"),
            set_replication=True,
            initdb_params=["--data-checksums"],
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # Create table with random (incompressible) data
        # Using md5() with random() to generate pseudo-random data without pgcrypto extension
        # Concatenating multiple md5 hashes creates ~500 bytes of poorly compressible data
        node.safe_psql(
            "postgres",
            "create table t_random as select i as id, "
            "decode("
            "md5(random()::text || i::text) || md5(random()::text || i::text) || "
            "md5(random()::text || i::text) || md5(random()::text || i::text) || "
            "md5(random()::text || i::text) || md5(random()::text || i::text) || "
            "md5(random()::text || i::text) || md5(random()::text || i::text) || "
            "md5(random()::text || i::text) || md5(random()::text || i::text) || "
            "md5(random()::text || i::text) || md5(random()::text || i::text) || "
            "md5(random()::text || i::text) || md5(random()::text || i::text) || "
            "md5(random()::text || i::text) || md5(random()::text || i::text), "
            "'hex') as random_data "
            "from generate_series(0,5000) i",
        )

        original_result = node.table_checksum("t_random")

        # Backup with LZ4
        backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="full",
            options=["--stream", "--compress-algorithm=lz4"],
        )

        # Validate backup
        self.validate_pb(backup_dir, "node", backup_id, options=["-j", "4"])

        # Check backup is OK
        backup_info = self.show_pb(backup_dir, "node", backup_id)
        self.assertEqual(backup_info["status"], "OK")

        # Restore and verify data integrity
        node.cleanup()
        self.restore_node(
            backup_dir,
            "node",
            node,
            backup_id=backup_id,
            options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
        )
        node.slow_start()

        restored_result = node.table_checksum("t_random")
        self.assertEqual(original_result, restored_result)

    def test_lz4_empty_and_sparse_tables(self):
        """
        Test LZ4 compression with edge cases: empty tables, sparse data
        """
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, "backup")
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, "node"),
            set_replication=True,
            initdb_params=["--data-checksums"],
        )

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, "node", node)
        self.set_archiving(backup_dir, "node", node)
        node.slow_start()

        # Create empty table
        node.safe_psql("postgres", "create table t_empty (id int, data text)")

        # Create table with NULLs (sparse data)
        node.safe_psql(
            "postgres",
            "create table t_sparse as select i as id, "
            "case when i % 100 = 0 then md5(i::text) else null end as data "
            "from generate_series(0,10000) i",
        )

        # Create table with repetitive data (highly compressible)
        node.safe_psql(
            "postgres",
            "create table t_repetitive as select i as id, repeat('A', 1000) as data from generate_series(0,1000) i",
        )

        empty_result = node.execute("postgres", "select count(*) from t_empty")[0][0]
        sparse_result = node.table_checksum("t_sparse")
        repetitive_result = node.table_checksum("t_repetitive")

        # FULL backup with LZ4
        full_backup_id = self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="full",
            options=["--stream", "--compress-algorithm=lz4"],
        )

        # Make changes and DELTA backup
        node.safe_psql("postgres", "insert into t_empty values (1, 'test')")
        node.safe_psql("postgres", "update t_sparse set data = 'updated' where id % 1000 = 0")

        self.backup_node(
            backup_dir,
            "node",
            node,
            backup_type="delta",
            options=["--stream", "--compress-algorithm=lz4"],
        )

        # Validate
        self.validate_pb(backup_dir, "node", options=["-j", "4"])

        # Restore from FULL and verify
        node.cleanup()
        self.restore_node(
            backup_dir,
            "node",
            node,
            backup_id=full_backup_id,
            options=["-j", "4", "--immediate", "--recovery-target-action=promote"],
        )
        node.slow_start()

        self.assertEqual(empty_result, node.execute("postgres", "select count(*) from t_empty")[0][0])
        self.assertEqual(sparse_result, node.table_checksum("t_sparse"))
        self.assertEqual(repetitive_result, node.table_checksum("t_repetitive"))
