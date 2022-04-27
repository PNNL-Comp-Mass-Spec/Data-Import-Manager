
xcopy Debug\DataImportManager.exe \\pnl\projects\OmicsSW\DMS_Programs\CaptureTaskManagerDistribution\DataImportManager\ /Y /D
xcopy Debug\DataImportManager.pdb \\pnl\projects\OmicsSW\DMS_Programs\CaptureTaskManagerDistribution\DataImportManager\ /Y /D
xcopy Debug\*.dll \\pnl\projects\OmicsSW\DMS_Programs\CaptureTaskManagerDistribution\DataImportManager\ /Y /D

xcopy Debug\DataImportManager.exe \\pnl\projects\OmicsSW\DMS_Programs\CaptureTaskManagerDistribution\DataImportManagerMan\ /Y /D
xcopy Debug\DataImportManager.pdb \\pnl\projects\OmicsSW\DMS_Programs\CaptureTaskManagerDistribution\DataImportManagerMan\ /Y /D
xcopy Debug\*.dll \\pnl\projects\OmicsSW\DMS_Programs\CaptureTaskManagerDistribution\DataImportManagerMan\ /Y /D

if not "%1"=="NoPause" pause
