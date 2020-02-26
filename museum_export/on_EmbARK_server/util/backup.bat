net use z: \\172.20.0.19\C$
sc \\172.20.0.19 stop "4D Server: EmbARK"
XCOPY "z:\EmbARK_Data\Datafile" "C:\Backup_from_server\" /Y
sc \\172.20.0.19 start "4D Server: EmbARK"
call C:\util\Refresh_Web_Kiosk_Datafile.bat
