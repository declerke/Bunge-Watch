"""pytest fixtures — HTML fixtures and sample bill PDF for offline testing."""
import os
import pytest

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

# Sample kenyalaw.org HTML (minimal valid table structure)
SAMPLE_KENYALAW_HTML = """
<html><body>
<table>
<tr><th>#</th><th>Bill</th><th>Sponsor</th><th>NA Bill No.</th>
    <th>Dated</th><th>Maturity</th><th>Gazette No.</th>
    <th>1st Read</th><th>2nd Read</th><th>3rd Read</th>
    <th>Remarks</th><th>Assent</th></tr>
<tr><td>1</td>
    <td><strong>The Data Protection (Amendment) Bill, 2025</strong>
        <a href="/kl/fileadmin/pdfdownloads/bills/2025/DataProtectionAmendment2025.pdf">PDF</a>
    </td>
    <td>Jane Mwangi</td>
    <td>NA Bill No. 7 of 2025</td>
    <td>15/03/25</td><td></td><td>Gazette No. 42</td>
    <td>18/03/25</td><td></td><td></td><td></td><td></td>
</tr>
<tr><td>2</td>
    <td><strong>The Artificial Intelligence (Regulation) Bill, 2025</strong></td>
    <td>John Kamau</td>
    <td>NA Bill No. 12 of 2025</td>
    <td>02/04/25</td><td></td><td>Gazette No. 78</td>
    <td>05/04/25</td><td>20/04/25</td><td></td><td></td><td></td>
</tr>
</table>
</body></html>
"""

SAMPLE_BILL_TEXT = """
KENYA GAZETTE SUPPLEMENT
THE DATA PROTECTION (AMENDMENT) BILL, 2025

ARRANGEMENT OF SECTIONS
1. Short title and commencement.
2. Amendment of section 2 of No. 24 of 2019.
3. Amendment of section 25 of No. 24 of 2019.

PART I — PRELIMINARY
1. This Act may be cited as the Data Protection (Amendment) Act, 2025
   and shall come into operation upon publication in the Gazette.

PART II — AMENDMENTS
2. Section 2 of the Data Protection Act is amended by inserting the
   following new definition: "artificial intelligence" means machine-based
   systems that can generate outputs such as predictions, recommendations,
   decisions or content.

3. A data controller who deploys an artificial intelligence system that
   processes personal data of a data subject shall notify the Data Commissioner
   within thirty days of such deployment and shall pay a fine not exceeding
   five million shillings for failure to comply.
"""


@pytest.fixture
def kenyalaw_html():
    return SAMPLE_KENYALAW_HTML


@pytest.fixture
def sample_bill_text():
    return SAMPLE_BILL_TEXT


@pytest.fixture
def sample_bill_pdf(tmp_path):
    """Create a minimal real PDF using reportlab if available, else return None."""
    try:
        from reportlab.pdfgen import canvas
        pdf_path = tmp_path / "sample_bill.pdf"
        c = canvas.Canvas(str(pdf_path))
        c.drawString(72, 720, "THE DATA PROTECTION (AMENDMENT) BILL, 2025")
        c.drawString(72, 700, "A Bill to amend the Data Protection Act, 2019.")
        c.drawString(72, 680, "Penalty: fine not exceeding five million shillings.")
        c.save()
        return str(pdf_path)
    except ImportError:
        return None
