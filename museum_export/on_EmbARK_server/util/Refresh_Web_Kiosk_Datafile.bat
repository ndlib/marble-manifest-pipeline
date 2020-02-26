REM Refresh_Web_Kiosk_Datafile.bat
NET STOP "EmbARK Web Kiosk (managed by AlwaysUpService)"
XCOPY "C:\Backup_from_server\Collection*.*" "C:\EmbARK_Web_Kiosk\Datafile\" /Y 
NET START "EmbARK Web Kiosk (managed by AlwaysUpService)"
