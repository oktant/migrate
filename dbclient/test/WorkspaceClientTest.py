import base64
import json
import logging
import os
import tempfile
import unittest
from unittest.mock import MagicMock

import wmconstants
from dbclient import WorkspaceClient
from thread_safe_writer import ThreadSafeWriter

WS_TEST_CONFIG = {
    'token': 'test_token',
    'url': 'test_url',
    'export_dir': './',
    'is_aws': True,
    'is_azure': False,
    'is_gcp': False,
    'skip_failed': True,
    'verbose': False,
    'verify_ssl': False,
    'file_format': 'DBC',
    'overwrite_notebooks': True,
    'checkpoint_dir': '/',
    'use_checkpoint': False,
    'profile': 'test_profile',
    'retry_total': 1,
    'retry_backoff': 2,
    'timeout': 60,
    'debug': False,
    'skip_missing_users': False,
    'skip_large_nb': False
}


def build_client(export_dir):
    configs = dict(WS_TEST_CONFIG, export_dir=export_dir)
    checkpoint_service = MagicMock()
    # the checkpoint key set is a no-op set, i.e. nothing has been exported / imported before
    checkpoint_service.get_checkpoint_key_set.return_value.contains.return_value = False
    return WorkspaceClient(configs, checkpoint_service)


class TestWorkspaceFiles(unittest.TestCase):

    def test_log_all_workspace_items_logs_non_notebook_files(self):
        with tempfile.TemporaryDirectory() as export_dir:
            export_dir += '/'
            ws_c = build_client(export_dir)
            ws_c.get = MagicMock(return_value={'objects': [
                {'path': '/data/nb', 'object_type': 'NOTEBOOK', 'object_id': 1},
                {'path': '/data/sales.csv', 'object_type': 'FILE', 'object_id': 2},
                {'path': '/data/report.xlsx', 'object_type': 'FILE', 'object_id': 3}
            ]})
            num_nbs = ws_c.log_all_workspace_items_entry(ws_path='/data')

            self.assertEqual(num_nbs, 1)
            with open(export_dir + 'workspace_files.log', 'r') as fp:
                logged = [json.loads(line) for line in fp]
            self.assertEqual([x.get('path') for x in logged], ['/data/sales.csv', '/data/report.xlsx'])
            # files must not end up in the notebook log, they are imported with a different format
            with open(export_dir + 'user_workspace.log', 'r') as fp:
                self.assertEqual([json.loads(line).get('path') for line in fp], ['/data/nb'])

    def test_download_workspace_files_keeps_original_filename(self):
        with tempfile.TemporaryDirectory() as export_dir:
            export_dir += '/'
            ws_c = build_client(export_dir)
            writer = ThreadSafeWriter(export_dir + 'workspace_files.log', 'w')
            writer.write(json.dumps({'path': '/data/sales.csv', 'object_id': 2}) + '\n')
            writer.close()
            ws_c.get = MagicMock(return_value={
                'content': base64.b64encode(b'a,b\n1,2\n').decode('utf-8'), 'file_type': 'csv'})

            num_files = ws_c.download_workspace_files()

            self.assertEqual(num_files, 1)
            # non-notebook files are exported with the AUTO format, DBC / SOURCE only apply to notebooks
            self.assertEqual(ws_c.get.call_args[0][1], {'path': '/data/sales.csv', 'format': 'AUTO'})
            saved_file = os.path.join(export_dir, 'file_artifacts', 'data', 'sales.csv')
            with open(saved_file, 'rb') as fp:
                self.assertEqual(fp.read(), b'a,b\n1,2\n')

    def test_download_workspace_files_without_log_is_a_noop(self):
        with tempfile.TemporaryDirectory() as export_dir:
            ws_c = build_client(export_dir + '/')
            ws_c.get = MagicMock()

            self.assertEqual(ws_c.download_workspace_files(), 0)
            ws_c.get.assert_not_called()

    def test_import_all_workspace_files(self):
        with tempfile.TemporaryDirectory() as export_dir:
            export_dir += '/'
            local_dir = os.path.join(export_dir, 'file_artifacts', 'data')
            os.makedirs(local_dir)
            with open(os.path.join(local_dir, 'sales.csv'), 'wb') as fp:
                fp.write(b'a,b\n1,2\n')
            ws_c = build_client(export_dir)
            ws_c.post = MagicMock(return_value={})

            ws_c.import_all_workspace_files()

            import_calls = [call for call in ws_c.post.call_args_list if call[0][0] == '/workspace/import']
            self.assertEqual(len(import_calls), 1)
            import_args = import_calls[0][0][1]
            # the workspace path of a file keeps its extension, unlike a notebook
            self.assertEqual(import_args['path'], '/data/sales.csv')
            self.assertEqual(import_args['format'], 'AUTO')
            self.assertEqual(base64.b64decode(import_args['content']), b'a,b\n1,2\n')

    def test_import_all_workspace_files_without_artifacts_is_a_noop(self):
        with tempfile.TemporaryDirectory() as export_dir:
            ws_c = build_client(export_dir + '/')
            ws_c.post = MagicMock()

            ws_c.import_all_workspace_files()
            ws_c.post.assert_not_called()


class TestExcludedPaths(unittest.TestCase):
    # the home directory of a service principal, which is named after its application id
    SP_HOME_PATTERN = r'^/Users/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/'

    def test_is_excluded_path(self):
        patterns = WorkspaceClient.compile_exclude_patterns([self.SP_HOME_PATTERN])

        self.assertTrue(WorkspaceClient.is_excluded_path(
            '/Users/8a1b3c4d-1111-2222-3333-444455556666/nb', exclude_patterns=patterns))
        self.assertFalse(WorkspaceClient.is_excluded_path(
            '/Users/foo@db.com/nb', exclude_patterns=patterns))
        # prefixes and patterns are both honoured, and no exclusion means nothing is skipped
        self.assertTrue(WorkspaceClient.is_excluded_path('/Shared/tmp/nb', exclude_prefixes=['/Shared/tmp']))
        self.assertFalse(WorkspaceClient.is_excluded_path('/Shared/tmp/nb'))

    def test_log_all_workspace_items_skips_matching_folders(self):
        listings = {
            '/Users': {'objects': [
                {'path': '/Users/foo@db.com', 'object_type': 'DIRECTORY', 'object_id': 1},
                {'path': '/Users/8a1b3c4d-1111-2222-3333-444455556666', 'object_type': 'DIRECTORY', 'object_id': 2}
            ]},
            '/Users/foo@db.com': {'objects': [
                {'path': '/Users/foo@db.com/nb', 'object_type': 'NOTEBOOK', 'object_id': 3},
                {'path': '/Users/foo@db.com/sales.csv', 'object_type': 'FILE', 'object_id': 4}
            ]},
            '/Users/8a1b3c4d-1111-2222-3333-444455556666': {'objects': [
                {'path': '/Users/8a1b3c4d-1111-2222-3333-444455556666/sp_nb', 'object_type': 'NOTEBOOK',
                 'object_id': 5},
                {'path': '/Users/8a1b3c4d-1111-2222-3333-444455556666/sp.csv', 'object_type': 'FILE', 'object_id': 6}
            ]}
        }
        with tempfile.TemporaryDirectory() as export_dir:
            export_dir += '/'
            ws_c = build_client(export_dir)
            ws_c.get = MagicMock(side_effect=lambda endpoint, args=None: listings.get(
                (args or {}).get('path'), {}))

            num_nbs = ws_c.log_all_workspace_items_entry(ws_path='/Users',
                                                         exclude_patterns=[self.SP_HOME_PATTERN])

            # only the notebook of the real user is logged, the service principal home is never listed
            self.assertEqual(num_nbs, 1)
            listed_paths = [call[0][1].get('path') for call in ws_c.get.call_args_list if len(call[0]) > 1]
            self.assertNotIn('/Users/8a1b3c4d-1111-2222-3333-444455556666', listed_paths)
            with open(export_dir + 'user_workspace.log') as fp:
                self.assertEqual([json.loads(line).get('path') for line in fp], ['/Users/foo@db.com/nb'])
            with open(export_dir + 'workspace_files.log') as fp:
                self.assertEqual([json.loads(line).get('path') for line in fp], ['/Users/foo@db.com/sales.csv'])
            with open(export_dir + 'user_dirs.log') as fp:
                self.assertEqual([json.loads(line).get('path') for line in fp], ['/Users/foo@db.com'])


if __name__ == '__main__':
    unittest.main()


class TestNotebookExportFailures(unittest.TestCase):

    def setUp(self):
        # get_error_logger adds a handler on every call, so drop the ones left behind by
        # other tests, which point at temp directories that no longer exist
        logger = logging.getLogger('workspace_migration_' + wmconstants.WORKSPACE_NOTEBOOK_OBJECT)
        for handler in logger.handlers:
            handler.close()
        logger.handlers.clear()

    def test_is_size_limit_error_matches_every_api_wording(self):
        self.assertTrue(WorkspaceClient.is_size_limit_error({'message': 'Size exceeds 10485760 bytes'}))
        self.assertTrue(WorkspaceClient.is_size_limit_error(
            {'message': 'The notebook at /Users/foo@db.com/nb has exceeded the memory limit 10485760 bytes. '
                        'Try clearing cell outputs or removing visualizations to reduce the size.'}))
        self.assertTrue(WorkspaceClient.is_size_limit_error(
            {'message': 'File size imported is (56018432 bytes), exceeded max size (10485760 bytes)'}))
        self.assertFalse(WorkspaceClient.is_size_limit_error({'message': "Path (/Users/foo@db.com/nb) doesn't exist."}))
        # a limit that is not about size must still be reported as a failure
        self.assertFalse(WorkspaceClient.is_size_limit_error(
            {'message': 'Request rate exceeded the allowed limit for this workspace'}))
        self.assertFalse(WorkspaceClient.is_size_limit_error({}))

    def test_download_notebooks_only_logs_actionable_failures(self):
        responses = {
            '/Users/foo@db.com/big': {
                'error_code': 'BAD_REQUEST', 'http_status_code': 400,
                'message': 'The notebook at /Users/foo@db.com/big has exceeded the memory limit 10485760 bytes.'},
            '/Users/foo@db.com/gone': {
                'error_code': 'RESOURCE_DOES_NOT_EXIST', 'http_status_code': 404,
                'message': "Path (/Users/foo@db.com/gone) doesn't exist."},
            '/Users/foo@db.com/denied': {
                'error_code': 'PERMISSION_DENIED', 'http_status_code': 403,
                'message': 'User does not have permission to access this object.'}
        }
        with tempfile.TemporaryDirectory() as export_dir:
            export_dir += '/'
            ws_c = build_client(export_dir)
            ws_c.skip_large_nb = True
            with open(export_dir + 'user_workspace.log', 'w') as fp:
                for path in responses:
                    fp.write(json.dumps({'path': path, 'object_id': 1}) + '\n')
            ws_c.get = MagicMock(side_effect=lambda endpoint, args: responses[args['path']])

            ws_c.download_notebooks()

            with open(export_dir + 'app_logs/failed_export_notebooks.log') as fp:
                failures = [line for line in fp if line.strip()]
            # an oversized notebook is skipped by --skip-large-nb, and a notebook deleted after the
            # workspace listing has nothing left to export, so neither may abort the pipeline
            self.assertEqual(len(failures), 1)
            self.assertIn('PERMISSION_DENIED', failures[0])

    def test_download_workspace_files_skips_oversized_files(self):
        logger = logging.getLogger('workspace_migration_' + wmconstants.WORKSPACE_FILE_OBJECT)
        for handler in logger.handlers:
            handler.close()
        logger.handlers.clear()
        with tempfile.TemporaryDirectory() as export_dir:
            export_dir += '/'
            ws_c = build_client(export_dir)
            ws_c.skip_large_nb = True
            with open(export_dir + 'workspace_files.log', 'w') as fp:
                fp.write(json.dumps({'path': '/Users/foo@db.com/big.csv', 'object_id': 1}) + '\n')
            ws_c.get = MagicMock(return_value={
                'error_code': 'BAD_REQUEST', 'http_status_code': 400,
                'message': 'File size imported is (56018432 bytes), exceeded max size (10485760 bytes)'})

            ws_c.download_workspace_files()

            with open(export_dir + 'app_logs/failed_export_workspace_files.log') as fp:
                self.assertEqual([line for line in fp if line.strip()], [])
