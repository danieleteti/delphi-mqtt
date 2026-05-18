program MethodPtrDemo;

{$APPTYPE CONSOLE}

{
  Method Pointer Example

  Demonstrates:
  - Using method pointers (of object) instead of anonymous methods
  - Traditional OOP event handling pattern
  - Class-based message handling

  Requirements: MQTT broker running on localhost:1883 (e.g., Mosquitto)
}

uses
  System.SysUtils,
  System.Classes,
  MQTT.Types in '..\..\src\MQTT.Types.pas',
  MQTT.Protocol in '..\..\src\MQTT.Protocol.pas',
  MQTT.Client in '..\..\src\MQTT.Client.pas';

type
  TMQTTDemo = class
  private
    FClient: IMQTTClient;
    FMessageCount: Integer;
    // Method pointer handlers (of object)
    procedure HandleMessage(const Topic: string; const Payload: TBytes);
    procedure HandleConnect(ReasonCode: TMQTTReasonCode; SessionPresent: Boolean);
    procedure HandleDisconnect(ReasonCode: TMQTTReasonCode; const ReasonString: string);
    procedure HandleError(const ErrorMsg: string);
  public
    constructor Create;
    procedure Run;
  end;

{ TMQTTDemo }

constructor TMQTTDemo.Create;
begin
  inherited Create;
  FMessageCount := 0;
  FClient := CreateMQTTClient;
end;

procedure TMQTTDemo.HandleMessage(const Topic: string; const Payload: TBytes);
begin
  Inc(FMessageCount);
  Writeln(Format('>>> [%d] Received on "%s": %s',
    [FMessageCount, Topic, TEncoding.UTF8.GetString(Payload)]));
end;

procedure TMQTTDemo.HandleConnect(ReasonCode: TMQTTReasonCode; SessionPresent: Boolean);
begin
  Writeln('Event: Connected with reason code ', Ord(ReasonCode),
    ' (sessionPresent=', BoolToStr(SessionPresent, True), ')');
end;

procedure TMQTTDemo.HandleDisconnect(ReasonCode: TMQTTReasonCode; const ReasonString: string);
begin
  Writeln('Event: Disconnected - ', ReasonString);
end;

procedure TMQTTDemo.HandleError(const ErrorMsg: string);
begin
  Writeln('Event: Error - ', ErrorMsg);
end;

procedure TMQTTDemo.Run;
begin
  Writeln('Delphi MQTT Client - Method Pointer Demo');
  Writeln('-----------------------------------------');
  Writeln;
  Writeln('This demo uses traditional method pointers (of object)');
  Writeln('instead of anonymous methods for event handling.');
  Writeln;

  // Set event handlers using method pointers
  FClient.SetOnConnect(HandleConnect);
  FClient.SetOnDisconnect(HandleDisconnect);
  FClient.SetOnError(HandleError);

  Writeln('Connecting to localhost...');
  FClient.Connect('localhost', 1883);
  Writeln('Connected!');
  Writeln;

  // Subscribe using method pointer
  Writeln('Subscribing to "delphi/methodptr/#"...');
  FClient.Subscribe('delphi/methodptr/#', HandleMessage, atLeastOnce);

  Sleep(500);

  // Publish test messages
  Writeln;
  Writeln('Publishing messages...');
  FClient.Publish('delphi/methodptr/test1', 'Hello from method pointer!', atMostOnce);
  FClient.Publish('delphi/methodptr/test2', 'Second message', atLeastOnce);
  FClient.Publish('delphi/methodptr/sub/deep', 'Deep topic message', atMostOnce);

  Writeln;
  Writeln('Waiting for messages... Press Enter to quit.');
  Readln;

  Writeln;
  Writeln(Format('Total messages received: %d', [FMessageCount]));

  FClient.Disconnect;
  Writeln('Disconnected.');
end;

var
  Demo: TMQTTDemo;
begin
  try
    Demo := TMQTTDemo.Create;
    try
      Demo.Run;
    finally
      Demo.Free;
    end;
  except
    on E: Exception do
      Writeln('Error: ', E.Message);
  end;
end.
