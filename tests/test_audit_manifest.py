import unittest
import tempfile
import os
import subprocess
import json
import csv
import shutil

class TestAuditManifest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.manifest_path = os.path.join(self.temp_dir, "manifest.csv")
        self.script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "scripts", "audit-manifest.sh")
        
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        
    def write_manifest(self, rows):
        with open(self.manifest_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["paper_element","claim","result_dir","command","raw_files","n_trials","limits"])
            writer.writerows(rows)
            
    def create_result_dir(self, name, verification=None, latency_data=None):
        dir_path = os.path.join(self.temp_dir, name)
        os.makedirs(dir_path, exist_ok=True)
        if verification is not None:
            with open(os.path.join(dir_path, "verification.json"), 'w') as f:
                json.dump(verification, f)
        if latency_data == 'csv':
            with open(os.path.join(dir_path, "latency_samples.csv"), 'w') as f:
                f.write("test")
        elif latency_data == 'json':
            with open(os.path.join(dir_path, "latency_summary.json"), 'w') as f:
                f.write("{}")
        return dir_path

    def run_audit(self):
        return subprocess.run([self.script_path, self.manifest_path], 
                              capture_output=True, text=True)

    def test_empty_manifest(self):
        self.write_manifest([])
        res = self.run_audit()
        self.assertEqual(res.returncode, 0)
        self.assertIn("Total checked: 0", res.stdout)
        
    def test_pass_all(self):
        d1 = self.create_result_dir("d1", verification={"verification": {"passed": True}}, latency_data='csv')
        d2 = self.create_result_dir("d2", verification={"verification": {"passed": True}})
        self.write_manifest([
            ["fig1", "sustained_latency", d1, "", "", "", ""],
            ["fig2", "functional_correctness", d2, "", "", "", ""]
        ])
        res = self.run_audit()
        self.assertEqual(res.returncode, 0)
        self.assertIn("Passed: 2", res.stdout)
        
    def test_missing_verification(self):
        d1 = self.create_result_dir("d1", verification=None)
        self.write_manifest([
            ["fig1", "functional_correctness", d1, "", "", "", ""]
        ])
        res = self.run_audit()
        self.assertEqual(res.returncode, 1)
        self.assertIn("verification.json is missing", res.stdout)
        
    def test_failed_verification(self):
        d1 = self.create_result_dir("d1", verification={"verification": {"passed": False}})
        self.write_manifest([
            ["fig1", "functional_correctness", d1, "", "", "", ""]
        ])
        res = self.run_audit()
        self.assertEqual(res.returncode, 1)
        self.assertIn("verification.passed is not true", res.stdout)
        
    def test_missing_latency(self):
        d1 = self.create_result_dir("d1", verification={"verification": {"passed": True}}, latency_data=None)
        self.write_manifest([
            ["fig1", "sustained_latency", d1, "", "", "", ""]
        ])
        res = self.run_audit()
        self.assertEqual(res.returncode, 1)
        self.assertIn("Missing latency data", res.stdout)
        
    def test_invalid_name(self):
        d1 = self.create_result_dir("_invalid_d1", verification={"verification": {"passed": True}})
        self.write_manifest([
            ["fig1", "functional_correctness", d1, "", "", "", ""]
        ])
        res = self.run_audit()
        self.assertEqual(res.returncode, 1)
        self.assertIn("Invalid or incomplete", res.stdout)

if __name__ == '__main__':
    unittest.main()
