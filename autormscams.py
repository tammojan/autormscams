#!/usr/bin/env python
#
# SPDX-FileCopyrightText: © 2021 Tammo Jan Dijkema <T.J.Dijkema@gmail.com>
#
# SPDX-License-Identifier: GPL-3.0-or-later

"""Automate confirming RMS nights and uploading results to CAMS"""

import ftplib
import logging
from glob import glob
import zipfile
import re
from os.path import join, basename, realpath, dirname
from datetime import datetime, timedelta
import configparser
import sys
import subprocess

logger = logging.getLogger(__name__)

config = configparser.ConfigParser(inline_comment_prefixes=[";"])
configfiles_read = config.read(
    [join(dirname(realpath(__file__)), "config.ini"), join(sys.prefix, "autormscams.ini")]
)

RMS_DIR = config["rms"]["rms_dir"]
FTPUSER = config["ftp"]["ftpuser"]
FTPPASSWORD = config["ftp"]["ftppassword"]
FTPSITE = config["ftp"]["ftpsite"]
FTPDIR = config["ftp"]["ftpdir"]
CMN_BINVIEWER = config["cmn_binviewer"]["cmn_binviewer"]


def get_ftpfilename(night_dir, camsid):
    """
    Get the right FTPDetectInfo
    """
    pattern = re.compile(rf".*FTPdetectinfo_{camsid:06d}[0-9_]*R?.txt")
    allftpfilenames = glob(join(night_dir, f"FTPdetectinfo_{camsid:06d}[0-9_]*.txt"))
    ftpfilenames = [filename for filename in allftpfilenames if pattern.match(filename)]
    assert(len(ftpfilenames) == 1)
    return ftpfilenames[0]


def upload_night(night_dir, camsid, sequenceid=1):
    """
    Upload one night of detections. The appropriate directory will be created if necessary.

    Args:
        night_dir (str): Full path to the night directory
        camsid (int): CAMS station id
        sequenceid (int): Normally 1, can be higher if more directories exist for one night.
    """
    try:
        calfilename = glob(join(night_dir, "CAL_*"))[0]
    except IndexError:
        print("No calfile found in", night_dir)
        return

    try:
        ftpfilename = get_ftpfilename(night_dir, camsid)
    except AssertionError:
        return

    startdatestr = night_dir.split("_")[-3]
    starttimestr = night_dir.split("_")[-2]

    startdate = datetime.strptime(startdatestr + "_" + starttimestr, "%Y%m%d_%H%M%S")

    zipfilename = f"{startdate:%Y_%m_%d}_{camsid:06d}_{startdate:%H_%M_%S}_{sequenceid:02d}.zip"
    zipfilepath = join(night_dir, zipfilename)

    with zipfile.ZipFile(zipfilepath, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(calfilename, arcname=basename(calfilename))
        zf.write(ftpfilename, arcname=basename(ftpfilename))

    with ftplib.FTP(host=FTPSITE, user=FTPUSER, passwd=FTPPASSWORD) as ftp:
        ftp.cwd(FTPDIR)

        #try:
        #    ftp.mkd(f"{startdate:%Y}")
        #except ftplib.error_perm:
        #    pass

        #ftp.cwd(f"{startdate:%Y}")

        #try:
        #    ftp.mkd(f"{startdate:%Y_%m}")
        #except ftplib.error_perm:
        #    pass

        #ftp.cwd(f"{startdate:%Y_%m}")

        with open(zipfilepath, "rb") as zf:
            ftp.storbinary(f"STOR {zipfilename}", fp=zf)


def get_uploaded_days(camsid, year, month):
    """
    Get a list of all uploaded days for one month.

    Args:
        station id (int)
        year (int)
        month (int)

    Returns:
        list of integers (day numbers)
    """
    logger.debug(f"Fetching already uploaded days in {year}-{month:02} for {rmsid}")
    pattern = re.compile(rf"^{year}_?{month:02}_?(\d\d)_{camsid:06d}_.*\.zip$")
    uploaded_days = []
    with ftplib.FTP(host=FTPSITE, user=FTPUSER, passwd=FTPPASSWORD) as ftp:
        ftp.cwd(FTPDIR)

        # Look for files in FTPDIR
        for name, _ in ftp.mlsd():
            match = pattern.match(name)
            if match:
                uploaded_days.append(int(match.groups(0)[0]))

        # Look for files in FTPDIR/YEAR/
        try:
            ftp.cwd(f"{year}")
        except ftplib.error_perm:
            print(f"Dirname {year}_{month:02d} missing")
            return uploaded_days

        for name, _ in ftp.mlsd():
            match = pattern.match(name)
            if match:
                print(f"Found {int(match.groups(0)[0])} in FTPDIR/YEAR")
                uploaded_days.append(int(match.groups(0)[0]))

        # Look for files in FTPDIR/YEAR/YEAR_MONTH
        try:
            ftp.cwd(f"{year}_{month:02d}")
        except ftplib.error_perm:
            return uploaded_days

        for name, _ in ftp.mlsd():
            match = pattern.match(name)
            if match:
                uploaded_days.append(int(match.groups(0)[0]))

    return uploaded_days


def get_num_detections(night_dir, camsid):
    """
    Get the number of detections for one night out of the CAMS-FTPdetectinfo file
    
    Returns -1 in case an FTPdetectinfo file could not be found
    """
    try:
        ftpfilename = get_ftpfilename(night_dir, camsid)
    except AssertionError:
        logger.critical(f"Could not find FTPdetectinfo file in {night_dir}")
        return -1

    with open(ftpfilename, "r") as f:
        first_line = f.readline()
    num_detections = int(first_line.split()[-1])
    return num_detections


def get_camsid(rmsid):
    """
    Extracts the CAMS id from some RMS config file
    """
    configfile = sorted(glob(join(RMS_DIR, f"ArchivedFiles/{rmsid}_*/.config")))[-1]

    rmsconfig = configparser.RawConfigParser(inline_comment_prefixes=[";"], strict=True)
    rmsconfig.read(configfile)
    return int(rmsconfig["System"]["cams_code"])


def main(year, month, rmsid):
    """
    Upload all files for one month (that were not yet uploaded). Not-confirmed directories
    will be reported (in a future version CMN_binviewer could be started).
    """

    camsid = get_camsid(rmsid)

    already_uploaded_days = get_uploaded_days(camsid, year, month)

    confirmed_dirs = sorted(glob(join(RMS_DIR, "ConfirmedFiles", f"{rmsid}_{year}{month:02d}*")))
    confirmed_dirs = [basename(confirmed_dir) for confirmed_dir in confirmed_dirs]
    archived_dirs = sorted(glob(join(RMS_DIR, "ArchivedFiles", f"{rmsid}_{year}{month:02d}*")))
    archived_dirs = [basename(archived_dir) for archived_dir in archived_dirs]
    sequence_ids = {}

    # Handle nights that are not confirmed
    for archived_dir in archived_dirs:
        archived_date = datetime.strptime(archived_dir.split("_")[-3], "%Y%m%d")
        if archived_date.day in already_uploaded_days:
            sequence_ids[archived_date] = sequence_ids.get(archived_date, 0) + 1
            continue

        if archived_dir in confirmed_dirs:
            continue

        num_detections = get_num_detections(join(RMS_DIR, "ArchivedFiles", archived_dir), camsid)

        if num_detections == 0:
            logger.info(f"Uploading zero: {archived_dir}")
            sequence_ids[archived_date] = sequence_ids.get(archived_date, 0) + 1
            sequenceid = sequence_ids[archived_date]
            upload_night(join(RMS_DIR, "ArchivedFiles", archived_dir), camsid, sequenceid)
            continue

        print(f"To be confirmed :", join(RMS_DIR, "ArchivedFiles", archived_dir), f"({num_detections} detections)")
        do_confirm = input("Confirm now? ")
        if do_confirm[0].lower() == "y":
            try:
                # Try new option with explicit FTPDetectInfo
                ftpdetectfiles = glob(
                    join(RMS_DIR, "ArchivedFiles", archived_dir, f"FTPdetectinfo_{rmsid}_????????_??????_??????.txt")
                )
                if len(ftpdetectfiles) != 1:
                    print("len(ftpdetectfiles) =", len(ftpdetectfiles), "in", archived_dir)
                    continue

                subprocess.run(
                    [
                        CMN_BINVIEWER,
                        "--confirmation",
                        join(RMS_DIR, "ArchivedFiles", archived_dir),
                        "--ftpdetectfile",
                        ftpdetectfiles[0],
                    ]
                )
            except:
                raise
                logging.info("Using CMN_binviewer without the new --ftpdetectfile option")
                subprocess.run([CMN_BINVIEWER, "-c", join(RMS_DIR, "ArchivedFiles", archived_dir)])

    # Upload confirmed files
    # Repopulate this array to include the newly confirmed directories
    confirmed_dirs = sorted(glob(join(RMS_DIR, "ConfirmedFiles", f"{rmsid}_{year}{month:02d}*")))
    confirmed_dirs = [basename(confirmed_dir) for confirmed_dir in confirmed_dirs]
    for confirmed_dir in confirmed_dirs:
        if len(glob(join(RMS_DIR, "ConfirmedFiles", confirmed_dir, "*"))) == 0:
            # HACK: CMN_binviewer sometimes creates empty directories, skip those
            logger.info(f"Skipping {confirmed_dir} because it's empty")
            continue
        confirmed_date = datetime.strptime(confirmed_dir.split("_")[-3], "%Y%m%d")
        # Increment sequenceid
        sequence_ids[confirmed_date] = sequence_ids.get(confirmed_date, 0) + 1
        num_detections = get_num_detections(join(RMS_DIR, "ConfirmedFiles", confirmed_dir), camsid)
        if confirmed_date.day not in already_uploaded_days:
            logger.info(f"Uploading {confirmed_dir} ({num_detections} detections)")
            # TODO: check sequenceid
            sequenceid = sequence_ids[confirmed_date]
            upload_night(join(RMS_DIR, "ConfirmedFiles", confirmed_dir), camsid, sequenceid)


def start_cmn_binviewer(night_dir):
    subprocess.run([CMN_BINVIEWER, "-c", night_dir])


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG,
    )

    if len(sys.argv) > 1:
        rmsids = sys.argv[1:]
    else:
        rmsids = config["rms"]["rmsids"].split(",")

    now = datetime.now()
    for rmsid in rmsids:
        lastmonth = now.replace(day=1) - timedelta(days=1)
        main(lastmonth.year, lastmonth.month, rmsid)
        main(now.year, now.month, rmsid)
