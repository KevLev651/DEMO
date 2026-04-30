from io import BytesIO
import os
import re
import time
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("PIDTOOL_FEEDBACK_DB_PATH", str(Path(tempfile.gettempdir()) / "stellar_test_feedback.sqlite3"))

from portal.app import app, FEEDBACK_DB_PATH, IFB_GMP_RUNS, IFB_GMP_RUNS_LOCK, load_feedback
from tools.scanner.pid_common import known_sheet_fallbacks


def extract_csrf(html):
    match = re.search(r'name="csrf_token" value="([^"]+)"', html)
    if not match:
        raise AssertionError("CSRF token not found in rendered HTML.")
    return match.group(1)


def pdf_zip_bytes(*names):
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in names:
            archive.writestr(name, b"%PDF-1.4\n%%EOF")
    buffer.seek(0)
    return buffer


class PortalAppTests(unittest.TestCase):
    def setUp(self):
        app.config.update(TESTING=True)
        for path in [
            FEEDBACK_DB_PATH,
            Path(str(FEEDBACK_DB_PATH) + "-wal"),
            Path(str(FEEDBACK_DB_PATH) + "-shm"),
        ]:
            if path.exists():
                path.unlink()
        with IFB_GMP_RUNS_LOCK:
            IFB_GMP_RUNS.clear()
        self.client = app.test_client()

    def login(self):
        response = self.client.get("/login")
        token = extract_csrf(response.get_data(as_text=True))
        return self.client.post(
            "/login",
            data={
                "username": "StellarProcess",
                "password": "Jax2026",
                "csrf_token": token,
            },
        )

    def test_login_page_renders(self):
        response = self.client.get("/login")
        self.assertEqual(response.status_code, 200)
        self.assertIn("csrf_token", response.get_data(as_text=True))

    def test_static_asset_serves(self):
        response = self.client.get("/static/img/stellar-star.png")
        try:
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.content_type, "image/png")
        finally:
            response.close()

    def test_private_index_redirects_to_login(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.location)

    def test_login_requires_valid_csrf(self):
        response = self.client.post(
            "/login",
            data={"username": "StellarProcess", "password": "Jax2026"},
        )
        self.assertEqual(response.status_code, 400)

    def test_login_with_csrf_allows_portal(self):
        response = self.login()
        self.assertEqual(response.status_code, 302)
        portal = self.client.get("/")
        self.assertEqual(portal.status_code, 200)
        self.assertIn("Process Tools", portal.get_data(as_text=True))

    def test_feedback_requires_csrf(self):
        self.login()
        response = self.client.post(
            "/feedback",
            data={
                "source": "portal",
                "category": "Suggestion",
                "message": "missing token",
            },
        )
        self.assertEqual(response.status_code, 400)

    def test_feedback_round_trip_uses_sqlite(self):
        self.login()
        portal = self.client.get("/")
        token = extract_csrf(portal.get_data(as_text=True))
        response = self.client.post(
            "/feedback",
            data={
                "source": "portal",
                "category": "Suggestion",
                "message": "route test",
                "csrf_token": token,
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, {"success": True})
        entries = load_feedback()
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["message"], "route test")

    def test_default_upload_limit_allows_large_drawing_demo(self):
        self.assertGreaterEqual(app.config["MAX_CONTENT_LENGTH"], 512 * 1024 * 1024)

    def test_ifb_gmp_large_upload_returns_json_error(self):
        original_limit = app.config["MAX_CONTENT_LENGTH"]
        try:
            self.login()
            page = self.client.get("/tools/pdf-revision-comparison")
            token = extract_csrf(page.get_data(as_text=True))
            app.config["MAX_CONTENT_LENGTH"] = 1
            response = self.client.post(
                "/ifb-gmp/start",
                data={
                    "csrf_token": token,
                    "rev_a_uploads": (BytesIO(b"too large"), "rev-a.zip"),
                    "rev_b_uploads": (BytesIO(b"too large"), "rev-b.zip"),
                },
                content_type="multipart/form-data",
            )
            self.assertEqual(response.status_code, 413)
            self.assertEqual(response.content_type, "application/json")
            self.assertIn("Upload is too large", response.json["error"])
        finally:
            app.config["MAX_CONTENT_LENGTH"] = original_limit

    def test_job_download_rejects_invalid_filename(self):
        self.login()
        response = self.client.get("/download/not-a-job/..%5Csecret.txt")
        self.assertEqual(response.status_code, 404)

    def test_scan_route_returns_download_link_with_mocked_processor(self):
        self.login()
        page = self.client.get("/tools/scanner")
        token = extract_csrf(page.get_data(as_text=True))
        captured_kwargs = {}

        def fake_scan(pdf_path, **_kwargs):
            captured_kwargs.update(_kwargs)
            output_path = Path(pdf_path).with_name("mock_scan_report.xlsx")
            output_path.write_bytes(b"mock workbook")
            return {
                "output_excel": str(output_path),
                "total_tags": 2,
                "unique_tags": 2,
                "unique_sheets": 1,
            }

        with patch("portal.app.run_scan", side_effect=fake_scan):
            response = self.client.post(
                "/scan",
                data={
                    "csrf_token": token,
                    "pdf": (BytesIO(b"%PDF-1.4\n%%EOF"), "sample.pdf"),
                },
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Scanner Complete", body)
        self.assertIn("Download Excel Report", body)
        self.assertEqual(captured_kwargs.get("ocr_mode"), "auto")

    def test_compare_route_returns_download_links_with_mocked_processor(self):
        self.login()
        page = self.client.get("/tools/comparator")
        token = extract_csrf(page.get_data(as_text=True))
        captured_kwargs = {}

        def fake_compare(pdf_path, xlsx_path, **_kwargs):
            captured_kwargs.update(_kwargs)
            output_pdf = Path(pdf_path).with_name("mock_annotated.pdf")
            output_excel = Path(xlsx_path).with_name("mock_annotated.xlsx")
            output_pdf.write_bytes(b"mock pdf")
            output_excel.write_bytes(b"mock workbook")
            return {
                "output_pdf": str(output_pdf),
                "output_xlsx": str(output_excel),
                "pages_mapped": 1,
                "matched": 1,
            }

        with patch("portal.app.run_comparison", side_effect=fake_compare):
            response = self.client.post(
                "/compare",
                data={
                    "csrf_token": token,
                    "pdf": (BytesIO(b"%PDF-1.4\n%%EOF"), "sample.pdf"),
                    "xlsx": (BytesIO(b"mock xlsx"), "tags.xlsx"),
                },
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Comparison Complete", body)
        self.assertIn("Download Annotated Excel", body)
        self.assertIn("Download Annotated PDF", body)
        self.assertEqual(captured_kwargs.get("ocr_mode"), "auto")

    def test_compare_route_allows_excel_only_result(self):
        self.login()
        page = self.client.get("/tools/comparator")
        token = extract_csrf(page.get_data(as_text=True))

        def fake_compare(pdf_path, xlsx_path, **_kwargs):
            output_excel = Path(xlsx_path).with_name("mock_annotated.xlsx")
            output_excel.write_bytes(b"mock workbook")
            return {
                "output_pdf": None,
                "output_xlsx": str(output_excel),
                "pages_mapped": 1,
                "matched": 1,
                "matched_high": 1,
                "not_found": 0,
                "no_page": 0,
            }

        with patch("portal.app.run_comparison", side_effect=fake_compare):
            response = self.client.post(
                "/compare",
                data={
                    "csrf_token": token,
                    "pdf": (BytesIO(b"%PDF-1.4\n%%EOF"), "sample.pdf"),
                    "xlsx": (BytesIO(b"mock xlsx"), "tags.xlsx"),
                },
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Comparison Complete", body)
        self.assertIn("Download Annotated Excel", body)
        self.assertNotIn("Download Annotated PDF", body)

    def test_ifb_gmp_compare_exposes_only_public_downloads(self):
        self.login()
        page = self.client.get("/tools/pdf-revision-comparison")
        self.assertEqual(page.status_code, 200)
        token = extract_csrf(page.get_data(as_text=True))

        def fake_compare_job(request_payload, **_kwargs):
            ifb_folder = Path(request_payload["inputs"]["ifb_folder"])
            gmp_folder = Path(request_payload["inputs"]["gmp_folder"])
            self.assertTrue(any(ifb_folder.rglob("*.pdf")))
            self.assertTrue(any(gmp_folder.rglob("*.pdf")))
            output_dir = Path(request_payload["output_root"]) / request_payload["run_id"]
            output_dir.mkdir(parents=True, exist_ok=True)
            (output_dir / "bluebeam_review.pdf").write_bytes(b"mock bluebeam")
            (output_dir / "report.xlsx").write_bytes(b"mock workbook")
            (output_dir / "changed_pairs.zip").write_bytes(b"mock zip")
            (output_dir / "manifest.json").write_text("{}", encoding="utf-8")
            (output_dir / "logs").mkdir()
            return {"output_dir": str(output_dir)}

        with patch("portal.app.run_ifb_gmp_compare_job", side_effect=fake_compare_job):
            response = self.client.post(
                "/ifb-gmp/start",
                data={
                    "csrf_token": token,
                    "rev_a_uploads": (pdf_zip_bytes("IFB/DT0001.pdf"), "rev-a.zip"),
                    "rev_b_uploads": (pdf_zip_bytes("GMP/DT0001.pdf"), "rev-b.zip"),
                },
                content_type="multipart/form-data",
            )
            self.assertEqual(response.status_code, 202)
            job_id = response.json["job_id"]

            payload = None
            for _attempt in range(50):
                status_response = self.client.get(f"/ifb-gmp/status/{job_id}")
                self.assertEqual(status_response.status_code, 200)
                payload = status_response.json
                if payload["status"] == "complete":
                    break
                time.sleep(0.05)

        self.assertIsNotNone(payload)
        self.assertEqual(payload["status"], "complete")
        self.assertEqual(
            {download["name"] for download in payload["downloads"]},
            {"bluebeam_review.pdf", "report.xlsx", "changed_pairs.zip"},
        )

        for artifact_name in ["bluebeam_review.pdf", "report.xlsx", "changed_pairs.zip"]:
            download_response = self.client.get(f"/ifb-gmp/download/{job_id}/{artifact_name}")
            try:
                self.assertEqual(download_response.status_code, 200)
            finally:
                download_response.close()

        self.assertEqual(
            self.client.get(f"/ifb-gmp/download/{job_id}/manifest.json").status_code,
            404,
        )

    def test_ifb_gmp_compare_accepts_mixed_zip_and_pdf_uploads(self):
        self.login()
        page = self.client.get("/tools/pdf-revision-comparison")
        self.assertEqual(page.status_code, 200)
        token = extract_csrf(page.get_data(as_text=True))

        def fake_compare_job(request_payload, **_kwargs):
            ifb_folder = Path(request_payload["inputs"]["ifb_folder"])
            gmp_folder = Path(request_payload["inputs"]["gmp_folder"])
            self.assertTrue((ifb_folder / "IFB" / "DT0001.pdf").is_file())
            self.assertTrue((ifb_folder / "DT0002.pdf").is_file())
            self.assertTrue((gmp_folder / "DT0003.pdf").is_file())
            output_dir = Path(request_payload["output_root"]) / request_payload["run_id"]
            output_dir.mkdir(parents=True, exist_ok=True)
            (output_dir / "bluebeam_review.pdf").write_bytes(b"mock bluebeam")
            (output_dir / "report.xlsx").write_bytes(b"mock workbook")
            (output_dir / "changed_pairs.zip").write_bytes(b"mock zip")
            return {"output_dir": str(output_dir)}

        with patch("portal.app.run_ifb_gmp_compare_job", side_effect=fake_compare_job):
            response = self.client.post(
                "/ifb-gmp/start",
                data={
                    "csrf_token": token,
                    "rev_a_uploads": [
                        (pdf_zip_bytes("IFB/DT0001.pdf"), "rev-a.zip"),
                        (BytesIO(b"%PDF-1.4\n%%EOF"), "DT0002.pdf"),
                    ],
                    "rev_b_uploads": (BytesIO(b"%PDF-1.4\n%%EOF"), "DT0003.pdf"),
                },
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 202)

    def test_default_sheet_fallbacks_preserve_existing_behavior(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(known_sheet_fallbacks(), {"DG7002": 6})

    def test_sheet_fallbacks_are_configurable(self):
        with patch.dict(os.environ, {"PIDTOOL_SHEET_FALLBACKS": "DG7002:6, DG7003:7, bad, DGX:2, DG7004:no"}):
            self.assertEqual(known_sheet_fallbacks(), {"DG7002": 6, "DG7003": 7})


if __name__ == "__main__":
    unittest.main()
