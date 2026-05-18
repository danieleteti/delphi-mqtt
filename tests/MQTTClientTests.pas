unit MQTTClientTests;

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Generics.Collections,
  IdTCPClient,
  MQTT.Types,
  MQTT.Protocol,
  MQTT.Logger,
  MQTT.Client;

type
  [TestFixture]
  TMQTTClientTests = class
  private
    FBrokerHost: string;
    FBrokerPort: Word;
    FBrokerAvailable: Boolean;
    function NewClientID: string;
    function MakeOptions(const ClientID: string): TMQTTConnectOptions;
    procedure RequireBroker;
  public
    [Setup]
    procedure Setup;

    [Test]
    procedure Connect_Disconnect_Lifecycle;
    [Test]
    procedure IsConnected_False_AfterDisconnect;
    [Test]
    procedure Publish_QoS0_NoException;
    [Test]
    procedure PublishSync_QoS1_ReturnsTrue;
    [Test]
    procedure Subscribe_ReceivesPublishedMessage;
    [Test]
    procedure Subscribe_Wildcard_PlusMatches;
    [Test]
    procedure Subscribe_Wildcard_HashMatches;
    [Test]
    procedure OnPacketSent_FiresOnPublish;
    [Test]
    procedure OnPacketReceived_FiresOnSubAck;
    [Test]
    procedure OnSubscribeAck_FiresWithGrantedQoS;
    [Test]
    procedure Logger_CapturesConnectMessage;
    [Test]
    procedure Unsubscribe_StopsReceivingMessages;
    [Test]
    procedure Publish_QoS2_RoundTrip;
    [Test]
    procedure RetainFlag_PreservedOnBroker;
    [Test]
    procedure LastSessionPresent_False_OnCleanStart;
    [Test]
    procedure LastSessionPresent_True_OnSecondConnectWithCleanStartFalse;
    [Test]
    procedure RestoreSubscriptionHandler_RejectsInvalidFilter;
    [Test]
    procedure RestoreSubscriptionHandler_DoesNotSendSubscribeOnWire;
    [Test]
    procedure RestoreSubscriptionHandler_DispatchesQueuedMessagesAfterSessionRestore;
    [Test]
    procedure ReconnectParams_DefaultsAreSane;
    [Test]
    procedure ReconnectParams_SettersAccept_GettersReturn;
    [Test]
    procedure ReconnectInitialDelayMs_Rejects_NonPositive;
    [Test]
    procedure ReconnectMaxDelayMs_Rejects_NonPositive;
    [Test]
    procedure ReconnectJitterPercent_Rejects_OutOfRange;
  end;

implementation

const
  WAIT_MS_SHORT = 1000;
  WAIT_MS_LONG = 3000;

{ TMQTTClientTests }

procedure TMQTTClientTests.Setup;
var
  Probe: TIdTCPClient;
begin
  FBrokerHost := 'localhost';
  FBrokerPort := 1883;
  FBrokerAvailable := False;
  Probe := TIdTCPClient.Create(nil);
  try
    Probe.Host := FBrokerHost;
    Probe.Port := FBrokerPort;
    Probe.ConnectTimeout := 1500;
    try
      Probe.Connect;
      FBrokerAvailable := True;
      Probe.Disconnect;
    except
      FBrokerAvailable := False;
    end;
  finally
    Probe.Free;
  end;
end;

procedure TMQTTClientTests.RequireBroker;
begin
  if not FBrokerAvailable then
    Assert.Pass(Format('Skipped: MQTT broker not reachable on %s:%d', [FBrokerHost, FBrokerPort]));
end;

function TMQTTClientTests.NewClientID: string;
begin
  Result := 'DUnit-' + FormatDateTime('hhnnsszzz', Now) + '-' + IntToStr(Random(10000));
end;

function TMQTTClientTests.MakeOptions(const ClientID: string): TMQTTConnectOptions;
begin
  Result.SetDefaults;
  Result.ClientID := ClientID;
  Result.KeepAliveSec := 30;
  Result.CleanStart := True;
  Result.Version := MQTT5;
end;

procedure TMQTTClientTests.Connect_Disconnect_Lifecycle;
var
  Client: IMQTTClient;
  Opts: TMQTTConnectOptions;
begin
  RequireBroker;
  Client := CreateMQTTClient;
  Opts := MakeOptions(NewClientID);
  Client.Connect(FBrokerHost, FBrokerPort, Opts);
  Assert.IsTrue(Client.Connected, 'should be connected after Connect');
  Client.Disconnect;
  Assert.IsFalse(Client.Connected, 'should not be connected after Disconnect');
end;

procedure TMQTTClientTests.IsConnected_False_AfterDisconnect;
var
  Client: IMQTTClient;
begin
  RequireBroker;
  Client := CreateMQTTClient;
  Client.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
  Client.Disconnect;
  Assert.IsFalse(Client.Connected);
  Assert.AreEqual(Ord(Disconnected), Ord(Client.State));
end;

procedure TMQTTClientTests.Publish_QoS0_NoException;
var
  Client: IMQTTClient;
begin
  RequireBroker;
  Client := CreateMQTTClient;
  Client.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
  try
    Client.Publish('dunit/qos0', 'fire-and-forget', atMostOnce);
    Assert.Pass('no exception');
  finally
    Client.Disconnect;
  end;
end;

procedure TMQTTClientTests.PublishSync_QoS1_ReturnsTrue;
var
  Client: IMQTTClient;
  Acked: Boolean;
begin
  RequireBroker;
  Client := CreateMQTTClient;
  Client.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
  try
    Acked := Client.PublishSync('dunit/qos1', TEncoding.UTF8.GetBytes('sync'),
      atLeastOnce, False, 3000);
    Assert.IsTrue(Acked, 'broker did not ACK QoS1 publish in time');
  finally
    Client.Disconnect;
  end;
end;

procedure TMQTTClientTests.Subscribe_ReceivesPublishedMessage;
var
  Sub, Pub: IMQTTClient;
  Topic, Payload: string;
  GotEvent: TEvent;
  ReceivedPayload: string;
begin
  RequireBroker;
  Topic := 'dunit/exchange/' + IntToStr(Random(1000000));
  Payload := 'hello-' + IntToStr(Random(1000));
  GotEvent := TEvent.Create(nil, True, False, '');
  try
    Sub := CreateMQTTClient;
    Pub := CreateMQTTClient;
    Sub.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    Pub.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    try
      Sub.Subscribe(Topic,
        procedure(const T: string; const P: TBytes)
        begin
          ReceivedPayload := TEncoding.UTF8.GetString(P);
          GotEvent.SetEvent;
        end,
        atLeastOnce);
      Sleep(500); // let SUBACK settle

      Pub.PublishSync(Topic, TEncoding.UTF8.GetBytes(Payload), atLeastOnce, False, 2000);
      Assert.AreEqual(wrSignaled, GotEvent.WaitFor(WAIT_MS_LONG), 'message not received in time');
      Assert.AreEqual(Payload, ReceivedPayload);
    finally
      Pub.Disconnect;
      Sub.Disconnect;
    end;
  finally
    GotEvent.Free;
  end;
end;

procedure TMQTTClientTests.Subscribe_Wildcard_PlusMatches;
var
  Sub, Pub: IMQTTClient;
  GotEvent: TEvent;
  ReceivedTopic: string;
  BaseTopic: string;
begin
  RequireBroker;
  BaseTopic := 'dunit/wc/' + IntToStr(Random(1000000));
  GotEvent := TEvent.Create(nil, True, False, '');
  try
    Sub := CreateMQTTClient;
    Pub := CreateMQTTClient;
    Sub.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    Pub.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    try
      Sub.Subscribe(BaseTopic + '/+/value',
        procedure(const T: string; const P: TBytes)
        begin
          ReceivedTopic := T;
          GotEvent.SetEvent;
        end,
        atLeastOnce);
      Sleep(500);
      Pub.PublishSync(BaseTopic + '/kitchen/value', TEncoding.UTF8.GetBytes('x'),
        atLeastOnce, False, 2000);
      Assert.AreEqual(wrSignaled, GotEvent.WaitFor(WAIT_MS_LONG));
      Assert.AreEqual(BaseTopic + '/kitchen/value', ReceivedTopic);
    finally
      Pub.Disconnect;
      Sub.Disconnect;
    end;
  finally
    GotEvent.Free;
  end;
end;

procedure TMQTTClientTests.Subscribe_Wildcard_HashMatches;
var
  Sub, Pub: IMQTTClient;
  GotEvent: TEvent;
  BaseTopic: string;
  Count: Integer;
  Lock: TCriticalSection;
begin
  RequireBroker;
  BaseTopic := 'dunit/hash/' + IntToStr(Random(1000000));
  GotEvent := TEvent.Create(nil, True, False, '');
  Lock := TCriticalSection.Create;
  Count := 0;
  try
    Sub := CreateMQTTClient;
    Pub := CreateMQTTClient;
    Sub.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    Pub.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    try
      Sub.Subscribe(BaseTopic + '/#',
        procedure(const T: string; const P: TBytes)
        begin
          Lock.Enter;
          try
            Inc(Count);
            if Count >= 2 then
              GotEvent.SetEvent;
          finally
            Lock.Leave;
          end;
        end,
        atLeastOnce);
      Sleep(500);
      Pub.PublishSync(BaseTopic + '/a', TEncoding.UTF8.GetBytes('1'),
        atLeastOnce, False, 2000);
      Pub.PublishSync(BaseTopic + '/a/b/c', TEncoding.UTF8.GetBytes('2'),
        atLeastOnce, False, 2000);
      Assert.AreEqual(wrSignaled, GotEvent.WaitFor(WAIT_MS_LONG));
      Assert.IsTrue(Count >= 2);
    finally
      Pub.Disconnect;
      Sub.Disconnect;
    end;
  finally
    Lock.Free;
    GotEvent.Free;
  end;
end;

procedure TMQTTClientTests.OnPacketSent_FiresOnPublish;
var
  Client: IMQTTClient;
  PublishSent: Boolean;
  Lock: TCriticalSection;
begin
  RequireBroker;
  PublishSent := False;
  Lock := TCriticalSection.Create;
  try
    Client := CreateMQTTClient;
    Client.SetOnPacketSent(
      procedure(PT: TMQTTPacketType; const Raw: TBytes)
      begin
        Lock.Enter;
        try
          if PT = ptPublish then
            PublishSent := True;
        finally
          Lock.Leave;
        end;
      end);
    Client.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    try
      Client.Publish('dunit/packetevent', 'x', atMostOnce);
      Sleep(300);
      Assert.IsTrue(PublishSent, 'OnPacketSent did not fire for PUBLISH');
    finally
      Client.Disconnect;
    end;
  finally
    Lock.Free;
  end;
end;

procedure TMQTTClientTests.OnPacketReceived_FiresOnSubAck;
var
  Client: IMQTTClient;
  SubAckReceived: Boolean;
  Lock: TCriticalSection;
begin
  RequireBroker;
  SubAckReceived := False;
  Lock := TCriticalSection.Create;
  try
    Client := CreateMQTTClient;
    Client.SetOnPacketReceived(
      procedure(PT: TMQTTPacketType; const Raw: TBytes)
      begin
        Lock.Enter;
        try
          if PT = ptSubAck then
            SubAckReceived := True;
        finally
          Lock.Leave;
        end;
      end);
    Client.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    try
      Client.Subscribe('dunit/rxevent',
        procedure(const T: string; const P: TBytes) begin end,
        atLeastOnce);
      Sleep(800);
      Assert.IsTrue(SubAckReceived, 'OnPacketReceived did not fire for SUBACK');
    finally
      Client.Disconnect;
    end;
  finally
    Lock.Free;
  end;
end;

procedure TMQTTClientTests.OnSubscribeAck_FiresWithGrantedQoS;
var
  Client: IMQTTClient;
  GotEvent: TEvent;
  Granted: TArray<Byte>;
begin
  RequireBroker;
  GotEvent := TEvent.Create(nil, True, False, '');
  try
    Client := CreateMQTTClient;
    Client.SetOnSubscribeAck(
      procedure(PacketID: Word; const GrantedQoS: TArray<Byte>)
      begin
        Granted := Copy(GrantedQoS);
        GotEvent.SetEvent;
      end);
    Client.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    try
      Client.Subscribe('dunit/suback',
        procedure(const T: string; const P: TBytes) begin end,
        atLeastOnce);
      Assert.AreEqual(wrSignaled, GotEvent.WaitFor(WAIT_MS_LONG), 'OnSubscribeAck not fired');
      Assert.IsTrue(Length(Granted) >= 1, 'no granted QoS returned');
      Assert.IsTrue(Granted[0] < $80, 'broker rejected subscription');
    finally
      Client.Disconnect;
    end;
  finally
    GotEvent.Free;
  end;
end;

procedure TMQTTClientTests.Logger_CapturesConnectMessage;
var
  Client: IMQTTClient;
  Captured: TStringList;
  CaptureLock: TCriticalSection;
  AllText: string;
begin
  RequireBroker;
  Captured := TStringList.Create;
  CaptureLock := TCriticalSection.Create;
  try
    Client := CreateMQTTClient;
    Client.Logger := CreateProcLogger(
      procedure(Level: TMQTTLogLevel; const Msg: string)
      begin
        CaptureLock.Enter;
        try
          Captured.Add(Format('[%s] %s', [LogLevelName(Level), Msg]));
        finally
          CaptureLock.Leave;
        end;
      end,
      llDebug);

    Client.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    Sleep(200);
    Client.Disconnect;

    CaptureLock.Enter;
    try
      AllText := Captured.Text;
    finally
      CaptureLock.Leave;
    end;

    Assert.IsTrue(AllText.Contains('Connected'),
      'logger never received a "Connected" entry. Captured: ' + AllText);
    Assert.IsTrue(AllText.Contains('CONNECT'),
      'logger never received a "CONNECT" packet entry. Captured: ' + AllText);
  finally
    CaptureLock.Free;
    Captured.Free;
  end;
end;

procedure TMQTTClientTests.Unsubscribe_StopsReceivingMessages;
var
  Sub, Pub: IMQTTClient;
  GotEvent: TEvent;
  Topic: string;
  RxCount: Integer;
  Lock: TCriticalSection;
begin
  RequireBroker;
  Topic := 'dunit/unsub/' + IntToStr(Random(1000000));
  GotEvent := TEvent.Create(nil, True, False, '');
  Lock := TCriticalSection.Create;
  RxCount := 0;
  try
    Sub := CreateMQTTClient;
    Pub := CreateMQTTClient;
    Sub.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    Pub.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    try
      Sub.Subscribe(Topic,
        procedure(const T: string; const P: TBytes)
        begin
          Lock.Enter;
          try
            Inc(RxCount);
            GotEvent.SetEvent;
          finally
            Lock.Leave;
          end;
        end,
        atLeastOnce);
      Sleep(500);

      Pub.PublishSync(Topic, TEncoding.UTF8.GetBytes('one'), atLeastOnce, False, 2000);
      Assert.AreEqual(wrSignaled, GotEvent.WaitFor(WAIT_MS_LONG));

      Sub.Unsubscribe(Topic);
      Sleep(500);

      GotEvent.ResetEvent;
      Pub.PublishSync(Topic, TEncoding.UTF8.GetBytes('two'), atLeastOnce, False, 2000);
      // Should NOT receive
      Assert.AreEqual(wrTimeout, GotEvent.WaitFor(WAIT_MS_SHORT),
        'received message after unsubscribe');
      Assert.AreEqual(1, RxCount);
    finally
      Pub.Disconnect;
      Sub.Disconnect;
    end;
  finally
    Lock.Free;
    GotEvent.Free;
  end;
end;

procedure TMQTTClientTests.Publish_QoS2_RoundTrip;
var
  Sub, Pub: IMQTTClient;
  GotEvent: TEvent;
  Topic, Received: string;
begin
  RequireBroker;
  Topic := 'dunit/qos2/' + IntToStr(Random(1000000));
  GotEvent := TEvent.Create(nil, True, False, '');
  try
    Sub := CreateMQTTClient;
    Pub := CreateMQTTClient;
    Sub.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    Pub.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    try
      Sub.Subscribe(Topic,
        procedure(const T: string; const P: TBytes)
        begin
          Received := TEncoding.UTF8.GetString(P);
          GotEvent.SetEvent;
        end,
        exactlyOnce);
      Sleep(500);
      Pub.PublishSync(Topic, TEncoding.UTF8.GetBytes('exactly-once'),
        exactlyOnce, False, 5000);
      Assert.AreEqual(wrSignaled, GotEvent.WaitFor(WAIT_MS_LONG));
      Assert.AreEqual('exactly-once', Received);
    finally
      Pub.Disconnect;
      Sub.Disconnect;
    end;
  finally
    GotEvent.Free;
  end;
end;

procedure TMQTTClientTests.RetainFlag_PreservedOnBroker;
var
  Pub, Sub: IMQTTClient;
  GotEvent: TEvent;
  Topic, Received: string;
begin
  RequireBroker;
  Topic := 'dunit/retain/' + IntToStr(Random(1000000));
  GotEvent := TEvent.Create(nil, True, False, '');
  try
    // Publisher writes retained
    Pub := CreateMQTTClient;
    Pub.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    try
      Pub.PublishSync(Topic, TEncoding.UTF8.GetBytes('retained-value'),
        atLeastOnce, True, 2000);
    finally
      Pub.Disconnect;
    end;

    Sleep(300);

    // Late subscriber should still receive the retained message
    Sub := CreateMQTTClient;
    Sub.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    try
      Sub.Subscribe(Topic,
        procedure(const T: string; const P: TBytes)
        begin
          Received := TEncoding.UTF8.GetString(P);
          GotEvent.SetEvent;
        end,
        atLeastOnce);
      Assert.AreEqual(wrSignaled, GotEvent.WaitFor(WAIT_MS_LONG),
        'did not receive retained message');
      Assert.AreEqual('retained-value', Received);

      // Clean up retained message
      Sub.Disconnect;
    finally
    end;

    // Cleanup: publish empty retained to clear
    Pub := CreateMQTTClient;
    Pub.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    try
      Pub.Publish(Topic, '', atLeastOnce, True);
      Sleep(100);
    finally
      Pub.Disconnect;
    end;
  finally
    GotEvent.Free;
  end;
end;

procedure TMQTTClientTests.LastSessionPresent_False_OnCleanStart;
var
  Client: IMQTTClient;
  Opts: TMQTTConnectOptions;
begin
  RequireBroker;
  Client := CreateMQTTClient;
  Opts := MakeOptions(NewClientID);
  Opts.CleanStart := True;
  Client.Connect(FBrokerHost, FBrokerPort, Opts);
  try
    Assert.IsFalse(Client.LastSessionPresent,
      'CleanStart=True must always yield SessionPresent=False');
  finally
    Client.Disconnect;
  end;
end;

procedure TMQTTClientTests.LastSessionPresent_True_OnSecondConnectWithCleanStartFalse;
var
  Client: IMQTTClient;
  Opts: TMQTTConnectOptions;
  ClientID: string;
begin
  RequireBroker;
  ClientID := NewClientID;

  // First connect: prime a persistent session on the broker.
  Client := CreateMQTTClient;
  Opts := MakeOptions(ClientID);
  Opts.CleanStart := False;
  Opts.SessionExpiryInterval := 60;
  Client.Connect(FBrokerHost, FBrokerPort, Opts);
  Assert.IsFalse(Client.LastSessionPresent,
    'first connect with this ClientID should be a brand-new session');
  Client.Disconnect;
  Client := nil;

  // Second connect: broker must report SessionPresent=True for the same ClientID.
  Client := CreateMQTTClient;
  Opts := MakeOptions(ClientID);
  Opts.CleanStart := False;
  Opts.SessionExpiryInterval := 60;
  Client.Connect(FBrokerHost, FBrokerPort, Opts);
  try
    Assert.IsTrue(Client.LastSessionPresent,
      'broker should restore the session created by the previous connect');
  finally
    // Cleanup: tear the session down so it does not leak across runs.
    Client.Disconnect;
    Client := nil;
    Client := CreateMQTTClient;
    Opts := MakeOptions(ClientID);
    Opts.CleanStart := True;
    Client.Connect(FBrokerHost, FBrokerPort, Opts);
    Client.Disconnect;
  end;
end;

procedure TMQTTClientTests.RestoreSubscriptionHandler_RejectsInvalidFilter;
var
  Client: IMQTTClient;
  NoopHandler: TMQTTHandler;
begin
  // Callable before Connect: no broker round-trip needed for the validation path.
  Client := CreateMQTTClient;
  NoopHandler :=
    procedure(const Topic: string; const Payload: TBytes)
    begin
    end;
  Assert.WillRaise(
    procedure
    begin
      Client.RestoreSubscriptionHandler('bad/+filter', NoopHandler);
    end,
    EMQTTException);
end;

procedure TMQTTClientTests.RestoreSubscriptionHandler_DoesNotSendSubscribeOnWire;
var
  Client: IMQTTClient;
  SubscribePacketSeen: Boolean;
  NoopHandler: TMQTTHandler;
begin
  RequireBroker;
  SubscribePacketSeen := False;
  Client := CreateMQTTClient;
  Client.SetOnPacketSent(
    procedure(PacketType: TMQTTPacketType; const RawPacket: TBytes)
    begin
      if PacketType = ptSubscribe then
        SubscribePacketSeen := True;
    end);
  Client.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
  try
    NoopHandler :=
      procedure(const Topic: string; const Payload: TBytes)
      begin
      end;
    Client.RestoreSubscriptionHandler('dunit/restore/' + IntToStr(Random(1000000)),
      NoopHandler);
    Sleep(200); // give any rogue background send a chance to surface
    Assert.IsFalse(SubscribePacketSeen,
      'RestoreSubscriptionHandler must not emit a SUBSCRIBE packet on the wire');
  finally
    Client.Disconnect;
  end;
end;

procedure TMQTTClientTests.RestoreSubscriptionHandler_DispatchesQueuedMessagesAfterSessionRestore;
var
  Subscriber, Publisher: IMQTTClient;
  Opts: TMQTTConnectOptions;
  ClientID, Topic, Payload, Received: string;
  GotEvent: TEvent;
begin
  RequireBroker;
  ClientID := NewClientID;
  Topic := 'dunit/restore/' + IntToStr(Random(1000000));
  Payload := 'queued-' + IntToStr(Random(1000000));
  Received := '';
  GotEvent := TEvent.Create(nil, True, False, '');
  try
    // Phase 1: prime a persistent session with a subscription, then disconnect.
    Subscriber := CreateMQTTClient;
    Opts := MakeOptions(ClientID);
    Opts.CleanStart := False;
    Opts.SessionExpiryInterval := 60;
    Subscriber.Connect(FBrokerHost, FBrokerPort, Opts);
    Subscriber.Subscribe(Topic,
      procedure(const ATopic: string; const APayload: TBytes)
      begin
        // No-op: phase-1 subscriber only primes the broker-side subscription.
      end,
      atLeastOnce);
    Sleep(300); // let SUBACK land before disconnect
    Subscriber.Disconnect;
    Subscriber := nil;

    // Phase 2: a separate publisher fires a QoS 1 message while the subscriber is offline.
    Publisher := CreateMQTTClient;
    Publisher.Connect(FBrokerHost, FBrokerPort, MakeOptions(NewClientID));
    try
      Assert.IsTrue(
        Publisher.PublishSync(Topic, TEncoding.UTF8.GetBytes(Payload), atLeastOnce, False, 3000),
        'publisher did not receive PUBACK from broker');
    finally
      Publisher.Disconnect;
      Publisher := nil;
    end;

    // Phase 3: brand-new TMQTTClient instance, same ClientID. Bind the handler
    // BEFORE Connect so it is ready when the broker begins draining the queue.
    Subscriber := CreateMQTTClient;
    Subscriber.RestoreSubscriptionHandler(Topic,
      procedure(const ATopic: string; const APayload: TBytes)
      begin
        Received := TEncoding.UTF8.GetString(APayload);
        GotEvent.SetEvent;
      end);
    Opts := MakeOptions(ClientID);
    Opts.CleanStart := False;
    Opts.SessionExpiryInterval := 60;
    Subscriber.Connect(FBrokerHost, FBrokerPort, Opts);
    try
      Assert.IsTrue(Subscriber.LastSessionPresent,
        'broker must report SessionPresent=True for the persisted ClientID');
      Assert.AreEqual(wrSignaled, GotEvent.WaitFor(WAIT_MS_LONG),
        'queued message was not dispatched via the restored handler');
      Assert.AreEqual(Payload, Received);
    finally
      // Cleanup: tear the session down.
      Subscriber.Disconnect;
      Subscriber := nil;
      Subscriber := CreateMQTTClient;
      Opts := MakeOptions(ClientID);
      Opts.CleanStart := True;
      Subscriber.Connect(FBrokerHost, FBrokerPort, Opts);
      Subscriber.Disconnect;
    end;
  finally
    GotEvent.Free;
  end;
end;

procedure TMQTTClientTests.ReconnectParams_DefaultsAreSane;
var
  Client: IMQTTClient;
begin
  Client := CreateMQTTClient;
  Assert.AreEqual(1000, Client.ReconnectInitialDelayMs, 'default initial delay');
  Assert.AreEqual(30000, Client.ReconnectMaxDelayMs, 'default max delay');
  Assert.AreEqual(25, Client.ReconnectJitterPercent, 'default jitter percent');
end;

procedure TMQTTClientTests.ReconnectParams_SettersAccept_GettersReturn;
var
  Client: IMQTTClient;
begin
  Client := CreateMQTTClient;
  Client.ReconnectInitialDelayMs := 250;
  Client.ReconnectMaxDelayMs := 60000;
  Client.ReconnectJitterPercent := 10;
  Assert.AreEqual(250, Client.ReconnectInitialDelayMs);
  Assert.AreEqual(60000, Client.ReconnectMaxDelayMs);
  Assert.AreEqual(10, Client.ReconnectJitterPercent);

  // Zero jitter must be accepted (disables jitter).
  Client.ReconnectJitterPercent := 0;
  Assert.AreEqual(0, Client.ReconnectJitterPercent);
end;

procedure TMQTTClientTests.ReconnectInitialDelayMs_Rejects_NonPositive;
var
  Client: IMQTTClient;
begin
  Client := CreateMQTTClient;
  Assert.WillRaise(
    procedure
    begin
      Client.ReconnectInitialDelayMs := 0;
    end,
    EMQTTException);
  Assert.WillRaise(
    procedure
    begin
      Client.ReconnectInitialDelayMs := -100;
    end,
    EMQTTException);
end;

procedure TMQTTClientTests.ReconnectMaxDelayMs_Rejects_NonPositive;
var
  Client: IMQTTClient;
begin
  Client := CreateMQTTClient;
  Assert.WillRaise(
    procedure
    begin
      Client.ReconnectMaxDelayMs := 0;
    end,
    EMQTTException);
end;

procedure TMQTTClientTests.ReconnectJitterPercent_Rejects_OutOfRange;
var
  Client: IMQTTClient;
begin
  Client := CreateMQTTClient;
  Assert.WillRaise(
    procedure
    begin
      Client.ReconnectJitterPercent := -1;
    end,
    EMQTTException);
  Assert.WillRaise(
    procedure
    begin
      Client.ReconnectJitterPercent := 101;
    end,
    EMQTTException);
end;

initialization
  Randomize;
  TDUnitX.RegisterTestFixture(TMQTTClientTests);

end.
