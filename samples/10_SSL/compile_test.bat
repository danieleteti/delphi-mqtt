@echo off
call "C:\Program Files (x86)\Embarcadero\Studio\37.0\bin\rsvars.bat"
cd /d "C:\DEV\delphi-mqtt\samples\10_SSL"
echo Compiling TestAllBrokers... > compile_output.txt
dcc64 TestAllBrokers.dpr -U"..\..\src" -NSSystem;Winapi;System.Win >> compile_output.txt 2>&1
echo Done. >> compile_output.txt
