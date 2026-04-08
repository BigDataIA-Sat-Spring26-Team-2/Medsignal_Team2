"""
download_faers.py — FAERS quarterly data downloader

Usage:
    python download_faers.py --year 2023
    python download_faers.py --year 2023 --quarters 1 2 3 4
    python download_faers.py --year 2023 --output data/faers

Downloads FAERS ASCII quarterly ZIPs directly from the FDA portal
using the known URL pattern, unzips them, and places the four
target files (DEMO, DRUG, REAC, OUTC) in the correct folder
structure for faers_prep.py.
"""

import argparse
import sys
import time
import zipfile
from pathlib import Path
import requests

FDA_BASE_URL   = "https://fis.fda.gov/content/Exports"
TARGET_FILES   = {"DEMO", "DRUG", "REAC", "OUTC"}
DEFAULT_OUTPUT = Path("data/faers")
CHUNK_SIZE     = 1024 * 1024 
REQUEST_DELAY  = 1.0


def get_year_dir(output_root: Path, year: int) -> Path:
    """Returns the year-level directory e.g. data/faers/2023/"""
    return output_root / str(year)


def log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def log_section(msg: str) -> None:
    print(f"\n{'='*55}\n{msg}\n{'='*55}", flush=True)


def build_url(year: int, quarter: int) -> str:
    """
    Constructs the FDA FAERS ASCII ZIP download URL.
    Confirmed URL pattern from direct file inspection:
    https://fis.fda.gov/content/Exports/faers_ascii_2023q1.zip
    """
    return f"{FDA_BASE_URL}/faers_ascii_{year}q{quarter}.zip"


def get_quarter_label(year: int, quarter: int) -> str:
    """Returns label like '23Q1' used in FAERS filenames."""
    return f"{str(year)[-2:]}Q{quarter}"


def get_folder_label(year: int, quarter: int) -> str:
    """Returns folder name like '2023Q1'."""
    return f"{year}Q{quarter}"


def download_zip(url: str, dest: Path) -> bool:
    """
    Downloads a ZIP file from url to dest with progress reporting.
    Returns True on success, False on failure.
    """
    log(f"URL: {url}")

    try:
        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()

        total      = int(resp.headers.get("content-length", 0))
        downloaded = 0

        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded / total * 100
                        print(
                            f"\r    {downloaded/1e6:.1f} MB "
                            f"/ {total/1e6:.1f} MB "
                            f"({pct:.0f}%)",
                            end="",
                            flush=True,
                        )
        print()
        log(f"Saved: {dest}")
        return True

    except requests.HTTPError as e:
        log(f"[ERROR] HTTP {e.response.status_code} — "
            f"file may not exist for this quarter yet")
        if dest.exists():
            dest.unlink()
        return False

    except requests.RequestException as e:
        log(f"[ERROR] Download failed: {e}")
        if dest.exists():
            dest.unlink()
        return False


def extract_target_files(
    zip_path: Path,
    output_dir: Path,
) -> list[str]:
    """
    Extracts only DEMO, DRUG, REAC, OUTC files from the ZIP.
    FAERS ZIPs contain many files — we only need four.
    Returns list of successfully extracted filenames.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    extracted = []

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            all_names = zf.namelist()
            log(f"ZIP contains {len(all_names)} file(s)")

            for name in all_names:
                basename = Path(name).name.upper()
                for target in TARGET_FILES:
                    if (
                        basename.startswith(target)
                        and basename.endswith(".TXT")
                    ):
                        out_path = output_dir / Path(name).name
                        log(f"Extracting: {name}")
                        with zf.open(name) as src, \
                             open(out_path, "wb") as dst:
                            dst.write(src.read())
                        extracted.append(Path(name).name)
                        break

        return extracted

    except zipfile.BadZipFile as e:
        log(f"[ERROR] Bad ZIP file: {e}")
        return []


def validate_quarter(
    output_dir: Path,
    year: int,
    quarter: int,
) -> bool:
    """
    Confirms all four target files exist and are non-empty
    for a given quarter.
    """
    label   = get_quarter_label(year, quarter)
    missing = []

    for target in TARGET_FILES:
        candidates = list(output_dir.glob(f"{target}{label}.*"))
        if not candidates or all(
            f.stat().st_size == 0 for f in candidates
        ):
            missing.append(target)

    if missing:
        log(f"[WARN] Missing or empty: {missing}")
        return False

    log(f"Validation passed — all 4 files present")
    return True


def print_summary(
    year: int,
    results: dict,
    output_root: Path,
) -> None:
    log_section("DOWNLOAD SUMMARY")

    passed = [q for q, ok in results.items() if ok]
    failed = [q for q, ok in results.items() if not ok]

    print(f"  Year     : {year}")
    print(f"  Output   : {output_root.resolve()}")
    print(f"  Success  : {[f'Q{q}' for q in passed]}")

    if failed:
        print(f"  Failed   : {[f'Q{q}' for q in failed]}")
        print(
            f"\n  For failed quarters, download manually from:\n"
            f"  https://fis.fda.gov/extensions/FPD-QDE-FAERS/"
            f"FPD-QDE-FAERS.html\n"
            f"  and place files under:\n"
            f"  {output_root}/<year>Q<n>/"
        )
    else:
        print(f"\n  All quarters downloaded successfully.")
        print(f"\n  Folder structure:")
        for q in sorted(results.keys()):
            folder = output_root / get_folder_label(year, q)
            label  = get_quarter_label(year, q)
            for t in sorted(TARGET_FILES):
                print(f"    {folder}/{t}{label}.txt")

    print(
        f"\n  faers_prep.py reads from:\n"
        f"  {output_root}/<year>Q<n>/*.txt"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Download FAERS quarterly ASCII data from the FDA portal."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python download_faers.py --year 2023\n"
            "  python download_faers.py --year 2023 --quarters 1 2\n"
            "  python download_faers.py --year 2022 --output data/faers\n"
        ),
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year to download (e.g. 2023)",
    )
    parser.add_argument(
        "--quarters",
        type=int,
        nargs="+",
        choices=[1, 2, 3, 4],
        default=[1, 2, 3, 4],
        help="Quarters to download (default: all four)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output root directory (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--keep-zips",
        action="store_true",
        help="Keep downloaded ZIP files after extraction",
    )

    args = parser.parse_args()

    if args.year < 2004 or args.year > 2025:
        print(
            f"[ERROR] Year must be between 2004 and 2025. "
            f"Got: {args.year}"
        )
        sys.exit(1)

    log_section("MedSignal — FAERS Downloader")
    print(f"  Year     : {args.year}")
    print(f"  Quarters : {[f'Q{q}' for q in sorted(args.quarters)]}")
    print(f"  Output   : {args.output.resolve()}")

    year_dir = get_year_dir(args.output, args.year)
    year_dir.mkdir(parents=True, exist_ok=True)
    log(f"Year directory: {year_dir.resolve()}")

    results = {}

    for quarter in sorted(args.quarters):
        log_section(f"Downloading {args.year}Q{quarter}")

        folder_label = get_folder_label(args.year, quarter)
        output_dir   = year_dir / folder_label
        output_dir.mkdir(parents=True, exist_ok=True)

        if validate_quarter(output_dir, args.year, quarter):
            log(f"Already extracted — skipping")
            results[quarter] = True
            continue


        url      = build_url(args.year, quarter)
        zip_path = year_dir / f"faers_ascii_{args.year}q{quarter}.zip"

        ok = download_zip(url, zip_path)
        if not ok:
            results[quarter] = False
            continue

        time.sleep(REQUEST_DELAY)

        extracted = extract_target_files(zip_path, output_dir)

        if not extracted:
            log(f"[ERROR] No target files extracted")
            results[quarter] = False
        else:
            log(f"Extracted {len(extracted)} file(s): {extracted}")
            results[quarter] = validate_quarter(
                output_dir, args.year, quarter
            )

        if zip_path.exists():
            if args.keep_zips:
                log(f"Keeping ZIP: {zip_path}")
            else:
                zip_path.unlink()
                log(f"Removed ZIP")

        time.sleep(REQUEST_DELAY)

    print_summary(args.year, results, year_dir)

    failed = [q for q, ok in results.items() if not ok]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()