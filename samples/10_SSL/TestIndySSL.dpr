program TestIndySSL;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  IdTCPClient,
  IdGlobal,
  IdSSLOpenSSL,
  IdSSLOpenSSLHeaders;

var
  TCP: TIdTCPClient;
  SSL: TIdSSLIOHandlerSocketOpenSSL;
begin
  Writeln('Direct Indy SSL Test');
  Writeln('====================');
  Writeln;

  if not LoadOpenSSLLibrary then
  begin
    Writeln('OpenSSL not loaded!');
    Readln;
    Exit;
  end;
  Writeln('OpenSSL: ' + OpenSSLVersion);
  Writeln;

  TCP := TIdTCPClient.Create(nil);
  SSL := TIdSSLIOHandlerSocketOpenSSL.Create(nil);
  try
    // Configure SSL
    SSL.SSLOptions.Method := sslvTLSv1_2;
    SSL.SSLOptions.Mode := sslmClient;
    SSL.SSLOptions.VerifyMode := [];
    SSL.SSLOptions.VerifyDepth := 0;

    // Attach to TCP client
    TCP.IOHandler := SSL;
    TCP.Host := 'test.mosquitto.org';
    TCP.Port := 8883;
    TCP.ConnectTimeout := 10000;
    TCP.ReadTimeout := 5000;

    Writeln('Connecting to test.mosquitto.org:8883...');
    try
      TCP.Connect;
      Writeln('TCP+SSL Connected!');
      Writeln('Cipher: ' + SSL.SSLSocket.Cipher.Name);

      // Send MQTT CONNECT packet manually
      var ConnectPacket: TBytes;
      SetLength(ConnectPacket, 14);
      ConnectPacket[0] := $10;  // CONNECT packet type
      ConnectPacket[1] := 12;   // Remaining length
      ConnectPacket[2] := 0;    // Protocol name length MSB
      ConnectPacket[3] := 4;    // Protocol name length LSB
      ConnectPacket[4] := Ord('M');
      ConnectPacket[5] := Ord('Q');
      ConnectPacket[6] := Ord('T');
      ConnectPacket[7] := Ord('T');
      ConnectPacket[8] := 4;    // Protocol level (MQTT 3.1.1)
      ConnectPacket[9] := 2;    // Connect flags (Clean session)
      ConnectPacket[10] := 0;   // Keep alive MSB
      ConnectPacket[11] := 60;  // Keep alive LSB (60 seconds)
      ConnectPacket[12] := 0;   // Client ID length MSB
      ConnectPacket[13] := 0;   // Client ID length LSB (empty = let broker assign)

      Writeln('Sending MQTT CONNECT...');
      TCP.IOHandler.Write(TIdBytes(ConnectPacket), Length(ConnectPacket));

      Writeln('Waiting for CONNACK...');
      TCP.IOHandler.CheckForDataOnSource(5000);

      if not TCP.IOHandler.InputBufferIsEmpty then
      begin
        var Response: Byte := TCP.IOHandler.ReadByte;
        Writeln('Received packet type: $' + IntToHex(Response, 2));
        if (Response shr 4) = 2 then
          Writeln('SUCCESS! Got CONNACK!')
        else
          Writeln('Unexpected packet type');
      end
      else
        Writeln('No response from broker');

      TCP.Disconnect;
    except
      on E: Exception do
        Writeln('Error: ' + E.Message);
    end;

  finally
    SSL.Free;
    TCP.Free;
  end;

  Writeln;
  Writeln('Press Enter to exit...');
  Readln;
end.
