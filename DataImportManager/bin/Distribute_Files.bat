
xcopy Debug\net48\DataImportManager.exe \\pnl\projects\OmicsSW\DMS_Programs\CaptureTaskManagerDistribution\DataImportManager\ /Y /D
xcopy Debug\net48\DataImportManager.pdb \\pnl\projects\OmicsSW\DMS_Programs\CaptureTaskManagerDistribution\DataImportManager\ /Y /D
xcopy Debug\net48\*.dll \\pnl\projects\OmicsSW\DMS_Programs\CaptureTaskManagerDistribution\DataImportManager\ /Y /D

xcopy Debug\net48\DataImportManager.exe \\pnl\projects\OmicsSW\DMS_Programs\CaptureTaskManagerDistribution\DataImportManagerMan\ /Y /D
xcopy Debug\net48\DataImportManager.pdb \\pnl\projects\OmicsSW\DMS_Programs\CaptureTaskManagerDistribution\DataImportManagerMan\ /Y /D
xcopy Debug\net48\*.dll \\pnl\projects\OmicsSW\DMS_Programs\CaptureTaskManagerDistribution\DataImportManagerMan\ /Y /D

if not "%1"=="NoPause" pause
