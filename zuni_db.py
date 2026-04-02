import os
import sqlite3
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import pandas as pd


def get_db_path() -> str:
    """
    Resolve the Zuni.db location.

    Default: the parent folder of this Streamlit project (Desktop/Zuni.db).
    Override: set env var ZUNI_DB_PATH.
    """

    env_path = os.environ.get("ZUNI_DB_PATH")
    if env_path:
        return env_path

    project_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.abspath(os.path.join(project_dir, os.pardir))
    return os.path.join(parent_dir, "Zuni.db")


@contextmanager
def db_connect(db_path: Optional[str] = None) -> Iterator[sqlite3.Connection]:
    db_path = db_path or get_db_path()
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def init_db(conn: sqlite3.Connection) -> None:
    """
    Ensure missing ERP tables exist, without touching existing ERP data.
    """

    # Payroll: created only if missing
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS Staff (
            StaffID INTEGER PRIMARY KEY AUTOINCREMENT,
            StaffName TEXT NOT NULL,
            Designation TEXT,
            MonthlySalary REAL,
            JoiningDate DATE,
            Status TEXT DEFAULT 'Active'
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS PayrollRuns (
            RunID INTEGER PRIMARY KEY AUTOINCREMENT,
            PeriodStart DATE,
            PeriodEnd DATE,
            RunDate DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS PayrollLines (
            LineID INTEGER PRIMARY KEY AUTOINCREMENT,
            RunID INTEGER,
            StaffID INTEGER,
            EarningAmount REAL DEFAULT 0,
            DeductionAmount REAL DEFAULT 0,
            NetPay REAL DEFAULT 0,
            Notes TEXT,
            FOREIGN KEY (RunID) REFERENCES PayrollRuns(RunID),
            FOREIGN KEY (StaffID) REFERENCES Staff(StaffID)
        );
        """
    )

    # Vendors: optional alias if Customers isn't suitable for the user's workflow.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS Vendors (
            VendorID INTEGER PRIMARY KEY AUTOINCREMENT,
            VendorName TEXT NOT NULL,
            ContactNo TEXT,
            Address TEXT,
            AccountCode INTEGER,
            Notes TEXT
        );
        """
    )

    # -----------------------------
    # Inventory / Procurement / Sales / Bank & Cash (12 tables)
    # -----------------------------
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS OutwardGatePass (
            OGPID INTEGER PRIMARY KEY AUTOINCREMENT,
            ItemID INTEGER,
            VehicleNo TEXT,
            DriverName TEXT,
            Quantity REAL,
            OutwardDate DATETIME DEFAULT CURRENT_TIMESTAMP,
            Remarks TEXT,
            FOREIGN KEY (ItemID) REFERENCES Stores(ItemID)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS Assets (
            AssetID INTEGER PRIMARY KEY AUTOINCREMENT,
            AssetName TEXT NOT NULL,
            Category TEXT,
            PurchaseDate DATE,
            PurchaseCost REAL DEFAULT 0,
            Status TEXT DEFAULT 'Active',
            Notes TEXT
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS PurchaseInvoices (
            PurchaseID INTEGER PRIMARY KEY AUTOINCREMENT,
            VendorName TEXT,
            VendorID INTEGER,
            PurchaseDate DATE DEFAULT CURRENT_DATE,
            PurchaseType TEXT DEFAULT 'Feed',
            InvoiceNo TEXT,
            Remarks TEXT,
            TotalAmount REAL DEFAULT 0,
            PaidAmount REAL DEFAULT 0,
            PaymentMethod TEXT,
            FOREIGN KEY (VendorID) REFERENCES Vendors(VendorID)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS PurchaseInvoiceLines (
            LineID INTEGER PRIMARY KEY AUTOINCREMENT,
            PurchaseID INTEGER NOT NULL,
            LineType TEXT DEFAULT 'Feed',
            ItemID INTEGER,
            AssetID INTEGER,
            Quantity REAL DEFAULT 0,
            UnitCost REAL DEFAULT 0,
            LineTotal REAL DEFAULT 0,
            Notes TEXT,
            FOREIGN KEY (PurchaseID) REFERENCES PurchaseInvoices(PurchaseID),
            FOREIGN KEY (ItemID) REFERENCES Stores(ItemID),
            FOREIGN KEY (AssetID) REFERENCES Assets(AssetID)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS SalesInvoices (
            SaleID INTEGER PRIMARY KEY AUTOINCREMENT,
            CustomerID INTEGER,
            CustomerName TEXT,
            SaleDate DATE DEFAULT CURRENT_DATE,
            SaleType TEXT DEFAULT 'Milk',
            InvoiceNo TEXT,
            Remarks TEXT,
            TotalAmount REAL DEFAULT 0,
            PaidAmount REAL DEFAULT 0,
            PaymentMethod TEXT,
            FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS SalesInvoiceLines (
            LineID INTEGER PRIMARY KEY AUTOINCREMENT,
            SaleID INTEGER NOT NULL,
            LineType TEXT DEFAULT 'Milk',
            AnimalID INTEGER,
            Liters REAL DEFAULT 0,
            Quantity REAL DEFAULT 0,
            UnitPrice REAL DEFAULT 0,
            LineTotal REAL DEFAULT 0,
            Notes TEXT,
            FOREIGN KEY (SaleID) REFERENCES SalesInvoices(SaleID),
            FOREIGN KEY (AnimalID) REFERENCES Animals(AnimalID)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS MilkSales (
            MilkSaleID INTEGER PRIMARY KEY AUTOINCREMENT,
            CustomerID INTEGER,
            SaleDate DATE DEFAULT CURRENT_DATE,
            Liters REAL DEFAULT 0,
            RatePerLiter REAL DEFAULT 0,
            Amount REAL DEFAULT 0,
            PaidAmount REAL DEFAULT 0,
            PaymentMethod TEXT,
            Remarks TEXT,
            FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS AnimalSales (
            AnimalSaleID INTEGER PRIMARY KEY AUTOINCREMENT,
            AnimalID INTEGER NOT NULL,
            BuyerName TEXT,
            CustomerID INTEGER,
            SaleDate DATE DEFAULT CURRENT_DATE,
            SalePrice REAL DEFAULT 0,
            PaidAmount REAL DEFAULT 0,
            PaymentMethod TEXT,
            Remarks TEXT,
            FOREIGN KEY (AnimalID) REFERENCES Animals(AnimalID),
            FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS BankAccounts (
            BankAccountID INTEGER PRIMARY KEY AUTOINCREMENT,
            AccountName TEXT NOT NULL,
            BankName TEXT,
            AccountNo TEXT,
            OpeningBalance REAL DEFAULT 0,
            Notes TEXT
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS BankTransactions (
            BankTransID INTEGER PRIMARY KEY AUTOINCREMENT,
            BankAccountID INTEGER NOT NULL,
            TransDate DATE DEFAULT CURRENT_DATE,
            Description TEXT,
            Debit REAL DEFAULT 0,
            Credit REAL DEFAULT 0,
            Reference TEXT,
            FOREIGN KEY (BankAccountID) REFERENCES BankAccounts(BankAccountID)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS Payments (
            PaymentID INTEGER PRIMARY KEY AUTOINCREMENT,
            PayeeName TEXT,
            VendorID INTEGER,
            CustomerID INTEGER,
            PaymentDate DATE DEFAULT CURRENT_DATE,
            PaymentMethod TEXT DEFAULT 'Cash',
            BankAccountID INTEGER,
            Amount REAL DEFAULT 0,
            Purpose TEXT,
            Reference TEXT,
            FOREIGN KEY (VendorID) REFERENCES Vendors(VendorID),
            FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
            FOREIGN KEY (BankAccountID) REFERENCES BankAccounts(BankAccountID)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS Receipts (
            ReceiptID INTEGER PRIMARY KEY AUTOINCREMENT,
            PayerName TEXT,
            CustomerID INTEGER,
            ReceiptDate DATE DEFAULT CURRENT_DATE,
            ReceiptMethod TEXT DEFAULT 'Cash',
            BankAccountID INTEGER,
            Amount REAL DEFAULT 0,
            Purpose TEXT,
            Reference TEXT,
            FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
            FOREIGN KEY (BankAccountID) REFERENCES BankAccounts(BankAccountID)
        );
        """
    )

    conn.commit()


def fetch_df(conn: sqlite3.Connection, query: str, params: Optional[Sequence[Any]] = None) -> pd.DataFrame:
    params = params or []
    return pd.read_sql_query(query, conn, params=params)


def fetchall_dicts(conn: sqlite3.Connection, query: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
    params = params or []
    cur = conn.execute(query, params)
    return [dict(r) for r in cur.fetchall()]


def execute(
    conn: sqlite3.Connection,
    query: str,
    params: Optional[Sequence[Any]] = None,
) -> int:
    params = params or []
    cur = conn.execute(query, params)
    conn.commit()
    return int(cur.lastrowid or 0)


def rebuild_coa_current_balances(conn: sqlite3.Connection) -> None:
    """
    Recompute COA.CurrentBalance from COA.OpeningBalance + GeneralLedger (Debit - Credit).
    """

    if not table_exists(conn, "COA"):
        return
    if not table_exists(conn, "GeneralLedger"):
        return

    accounts = conn.execute("SELECT AccountCode, COALESCE(OpeningBalance, 0) AS OpeningBalance FROM COA").fetchall()
    for acc in accounts:
        code = acc["AccountCode"]
        opening = float(acc["OpeningBalance"] or 0)
        net = conn.execute(
            """
            SELECT COALESCE(SUM(COALESCE(Debit,0) - COALESCE(Credit,0)), 0)
            AS Net
            FROM GeneralLedger
            WHERE AccountCode=?
            """,
            (code,),
        ).fetchone()["Net"]
        current = opening + float(net or 0)
        conn.execute("UPDATE COA SET CurrentBalance=? WHERE AccountCode=?", (current, code))
    conn.commit()


def get_voucher_types(conn: sqlite3.Connection) -> List[str]:
    if not table_exists(conn, "Vouchers"):
        return ["JV", "Receipt", "Payment"]
    rows = conn.execute("SELECT DISTINCT VoucherType FROM Vouchers ORDER BY VoucherType").fetchall()
    types = [r[0] for r in rows if r[0]]
    return types or ["JV", "Receipt", "Payment"]


def resolve_vendors_source(conn: sqlite3.Connection) -> str:
    """
    Return the table name to use for Vendors.
    Priority: if `Vendors` has rows, use it; otherwise fall back to `Customers` if it exists.
    If neither exists, return `Vendors`.
    """

    if table_exists(conn, "Vendors"):
        cnt = conn.execute("SELECT COUNT(*) AS C FROM Vendors").fetchone()["C"]
        if int(cnt or 0) > 0:
            return "Vendors"
    if table_exists(conn, "Customers"):
        return "Customers"
    return "Vendors"

