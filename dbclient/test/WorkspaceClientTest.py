import base64
import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock

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


if __name__ == '__main__':
    unittest.main()
