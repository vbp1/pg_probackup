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
            backup_dir, "node", node, backup_type="full", options=["--stream", "--compress-algorithm=zlib"]
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
            backup_dir, "node", node, backup_type="page", options=["--stream", "--compress-algorithm=zlib"]
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
            backup_dir, "node", node, backup_type="delta", options=["--stream", "--compress-algorithm=zlib"]
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
            backup_dir, "node", node, backup_type="full", options=["--compress-algorithm=zlib"]
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
            backup_dir, "node", node, backup_type="page", options=["--compress-algorithm=zlib"]
        )

        # DELTA BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,3) i",
        )
        delta_result = node.table_checksum("t_heap")
        delta_backup_id = self.backup_node(
            backup_dir, "node", node, backup_type="delta", options=["--compress-algorithm=zlib"]
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
            backup_dir, "node", node, backup_type="full", options=["--stream", "--compress-algorithm=pglz"]
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
            backup_dir, "node", node, backup_type="page", options=["--stream", "--compress-algorithm=pglz"]
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
            backup_dir, "node", node, backup_type="delta", options=["--stream", "--compress-algorithm=pglz"]
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
            backup_dir, "node", node, backup_type="full", options=["--compress-algorithm=pglz"]
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
            backup_dir, "node", node, backup_type="page", options=["--compress-algorithm=pglz"]
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
            backup_dir, "node", node, backup_type="delta", options=["--compress-algorithm=pglz"]
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
            self.backup_node(backup_dir, "node", node, backup_type="full", options=["--compress-algorithm=bla-blah"])
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
        self.backup_node(backup_dir, "node", node, options=["--compress-algorithm=zlib", "--compress-level=0"])

        node.pgbench_init(scale=3)

        self.backup_node(
            backup_dir, "node", node, backup_type="delta", options=["--compress-algorithm=zlib", "--compress-level=0"]
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
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,256) i")
        full_result = node.table_checksum("t_heap")
        full_backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='full',
            options=[
                '--stream',
                '--compress-algorithm=lz4'])

        # PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(256,512) i")
        page_result = node.table_checksum("t_heap")
        page_backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page',
            options=[
                '--stream', '--compress-algorithm=lz4'])

        # DELTA BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(512,768) i")
        delta_result = node.table_checksum("t_heap")
        delta_backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='delta',
            options=['--stream', '--compress-algorithm=lz4'])

        # Drop Node
        node.cleanup()

        # Check full backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(
                backup_dir, 'node', node, backup_id=full_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd))
        node.slow_start()

        full_result_new = node.table_checksum("t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check page backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(page_backup_id),
            self.restore_node(
                backup_dir, 'node', node, backup_id=page_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd))
        node.slow_start()

        page_result_new = node.table_checksum("t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

        # Check delta backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(delta_backup_id),
            self.restore_node(
                backup_dir, 'node', node, backup_id=delta_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd))
        node.slow_start()

        delta_result_new = node.table_checksum("t_heap")
        self.assertEqual(delta_result, delta_result_new)

    def test_compression_archive_lz4(self):
        """
        make archive node, make full, page and delta backups with LZ4,
        check data correctness in restored instance
        """
        self.maxDiff = None
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # FULL BACKUP
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,1) i")
        full_result = node.table_checksum("t_heap")
        full_backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='full',
            options=["--compress-algorithm=lz4"])

        # PAGE BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector "
            "from generate_series(0,2) i")
        page_result = node.table_checksum("t_heap")
        page_backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='page',
            options=["--compress-algorithm=lz4"])

        # DELTA BACKUP
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(i::text)::tsvector as tsvector from generate_series(0,3) i")
        delta_result = node.table_checksum("t_heap")
        delta_backup_id = self.backup_node(
            backup_dir, 'node', node, backup_type='delta',
            options=['--compress-algorithm=lz4'])

        # Drop Node
        node.cleanup()

        # Check full backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(full_backup_id),
            self.restore_node(
                backup_dir, 'node', node, backup_id=full_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd))
        node.slow_start()

        full_result_new = node.table_checksum("t_heap")
        self.assertEqual(full_result, full_result_new)
        node.cleanup()

        # Check page backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(page_backup_id),
            self.restore_node(
                backup_dir, 'node', node, backup_id=page_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd))
        node.slow_start()

        page_result_new = node.table_checksum("t_heap")
        self.assertEqual(page_result, page_result_new)
        node.cleanup()

        # Check delta backup
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(delta_backup_id),
            self.restore_node(
                backup_dir, 'node', node, backup_id=delta_backup_id,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd))
        node.slow_start()

        delta_result_new = node.table_checksum("t_heap")
        self.assertEqual(delta_result, delta_result_new)
        node.cleanup()

    def test_lz4_compression_levels(self):
        """
        test LZ4 compression with different levels (0=fast, 1-12=HC)
        """
        self.maxDiff = None
        backup_dir = os.path.join(self.tmp_path, self.module_name, self.fname, 'backup')
        node = self.make_simple_node(
            base_dir=os.path.join(self.module_name, self.fname, 'node'),
            set_replication=True,
            initdb_params=['--data-checksums'])

        self.init_pb(backup_dir)
        self.add_instance(backup_dir, 'node', node)
        self.set_archiving(backup_dir, 'node', node)
        node.slow_start()

        # Create test data
        node.safe_psql(
            "postgres",
            "create table t_heap as select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(0,1000) i")
        original_result = node.table_checksum("t_heap")

        # Test level 0 (fast compression)
        backup_id_fast = self.backup_node(
            backup_dir, 'node', node, backup_type='full',
            options=[
                '--stream',
                '--compress-algorithm=lz4',
                '--compress-level=0'])

        # Test level 6 (medium HC compression)
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(1000,2000) i")
        result_level6 = node.table_checksum("t_heap")
        backup_id_level6 = self.backup_node(
            backup_dir, 'node', node, backup_type='delta',
            options=[
                '--stream',
                '--compress-algorithm=lz4',
                '--compress-level=6'])

        # Test level 12 (max HC compression)
        node.safe_psql(
            "postgres",
            "insert into t_heap select i as id, md5(i::text) as text, "
            "md5(repeat(i::text,10))::tsvector as tsvector "
            "from generate_series(2000,3000) i")
        result_level12 = node.table_checksum("t_heap")
        backup_id_level12 = self.backup_node(
            backup_dir, 'node', node, backup_type='delta',
            options=[
                '--stream',
                '--compress-algorithm=lz4',
                '--compress-level=12'])

        # Drop Node
        node.cleanup()

        # Check level 12 backup restore
        self.assertIn(
            "INFO: Restore of backup {0} completed.".format(backup_id_level12),
            self.restore_node(
                backup_dir, 'node', node, backup_id=backup_id_level12,
                options=[
                    "-j", "4", "--immediate",
                    "--recovery-target-action=promote"]),
            '\n Unexpected Error Message: {0}\n CMD: {1}'.format(
                repr(self.output), self.cmd))
        node.slow_start()

        restored_result = node.table_checksum("t_heap")
        self.assertEqual(result_level12, restored_result)
