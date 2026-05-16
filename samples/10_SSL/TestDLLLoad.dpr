program TestDLLLoad;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Winapi.Windows;

var
  hSSL, hCrypto: THandle;
  DLLPath: string;
begin
  Writeln('OpenSSL DLL Loading Test');
  Writeln('========================');
  Writeln;

  DLLPath := ExtractFilePath(ParamStr(0));
  Writeln('Executable path: ' + DLLPath);
  Writeln('Current dir: ' + GetCurrentDir);
  Writeln;

  // Try different DLL names
  Writeln('Trying to load DLLs...');
  Writeln;

  // Try libcrypto first (libssl depends on it)
  Writeln('1. Trying: libcrypto-1_1.dll');
  hCrypto := LoadLibrary('libcrypto-1_1.dll');
  if hCrypto <> 0 then
    Writeln('   SUCCESS!')
  else
    Writeln('   FAILED - Error: ' + IntToStr(GetLastError));

  Writeln('2. Trying: libcrypto-1_1-x64.dll');
  hCrypto := LoadLibrary('libcrypto-1_1-x64.dll');
  if hCrypto <> 0 then
    Writeln('   SUCCESS!')
  else
    Writeln('   FAILED - Error: ' + IntToStr(GetLastError));

  Writeln('3. Trying: ' + DLLPath + 'libcrypto-1_1.dll');
  hCrypto := LoadLibrary(PChar(DLLPath + 'libcrypto-1_1.dll'));
  if hCrypto <> 0 then
    Writeln('   SUCCESS!')
  else
    Writeln('   FAILED - Error: ' + IntToStr(GetLastError));

  Writeln;
  Writeln('4. Trying: libssl-1_1.dll');
  hSSL := LoadLibrary('libssl-1_1.dll');
  if hSSL <> 0 then
    Writeln('   SUCCESS!')
  else
    Writeln('   FAILED - Error: ' + IntToStr(GetLastError));

  Writeln('5. Trying: libssl-1_1-x64.dll');
  hSSL := LoadLibrary('libssl-1_1-x64.dll');
  if hSSL <> 0 then
    Writeln('   SUCCESS!')
  else
    Writeln('   FAILED - Error: ' + IntToStr(GetLastError));

  // Try OpenSSL 3.x names
  Writeln;
  Writeln('6. Trying: libcrypto-3.dll (OpenSSL 3.x)');
  hCrypto := LoadLibrary('libcrypto-3.dll');
  if hCrypto <> 0 then
    Writeln('   SUCCESS!')
  else
    Writeln('   FAILED - Error: ' + IntToStr(GetLastError));

  Writeln('7. Trying: libcrypto-3-x64.dll (OpenSSL 3.x)');
  hCrypto := LoadLibrary('libcrypto-3-x64.dll');
  if hCrypto <> 0 then
    Writeln('   SUCCESS!')
  else
    Writeln('   FAILED - Error: ' + IntToStr(GetLastError));

  // Try old OpenSSL 1.0.x names
  Writeln;
  Writeln('8. Trying: libeay32.dll (OpenSSL 1.0.x)');
  hCrypto := LoadLibrary('libeay32.dll');
  if hCrypto <> 0 then
    Writeln('   SUCCESS!')
  else
    Writeln('   FAILED - Error: ' + IntToStr(GetLastError));

  Writeln('9. Trying: ssleay32.dll (OpenSSL 1.0.x)');
  hSSL := LoadLibrary('ssleay32.dll');
  if hSSL <> 0 then
    Writeln('   SUCCESS!')
  else
    Writeln('   FAILED - Error: ' + IntToStr(GetLastError));

  Writeln;
  Writeln('Error codes: 126 = DLL not found, 193 = wrong architecture (32/64 bit mismatch)');
  Writeln;
  Writeln('Press Enter to exit...');
  Readln;
end.
