# autormscams

Automate confirming meteor detections from RMS and uploading results to CAMS

If you're running a RMS camera from the [GlobalMeteorNetwork](https://globalmeteornetwork.org),
and are in an area where also [CAMS](http://cams.seti.org/) has good coverage (e.g. the BeNeLux),
you may consider contributing meteor detections from your RMS camera to CAMS as well.

CAMS requires manual confirmation of all meteor detections. The tool to do these confirmations
is [CMN_binviewer](https://github.com/CroatianMeteorNetwork/cmn_binviewer). It goes through the
detections in a given night (typically a subdirectory of `RMS_data/ArchivedFiles`) and asks for
confirmation of each meteor.

AutoRMSCams automates going through many nights, and uploading the confirmed meteors to an FTP
site. It currently is tweaked to use the directory layout as used on the CAMS BeNeLux FTP server.
This tool is intended for Linux and macos systems; for Windows there are better CAMS alternatives.

To get started with this tool, edit the file `config.ini` to set some parameters.
