
xcopy Debug\net48\DataImportManager.exe \\Proto-3\DMS_Programs_Dist\CaptureTaskManagerDistribution\DataImportManager\ /Y /D
xcopy Debug\net48\DataImportManager.pdb \\Proto-3\DMS_Programs_Dist\CaptureTaskManagerDistribution\DataImportManager\ /Y /D
xcopy Debug\net48\*.dll \\Proto-3\DMS_Programs_Dist\CaptureTaskManagerDistribution\DataImportManager\ /Y /D

xcopy Debug\net48\DataImportManager.exe \\Proto-3\DMS_Programs_Dist\CaptureTaskManagerDistribution\DataImportManagerMan\ /Y /D
xcopy Debug\net48\DataImportManager.pdb \\Proto-3\DMS_Programs_Dist\CaptureTaskManagerDistribution\DataImportManagerMan\ /Y /D
xcopy Debug\net48\*.dll \\Proto-3\DMS_Programs_Dist\CaptureTaskManagerDistribution\DataImportManagerMan\ /Y /D

if not "%1"=="NoPause" pause
