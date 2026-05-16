unit MQTTProtocolTests;

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  MQTT.Types,
  MQTT.Protocol;

type
  [TestFixture]
  TMQTTProtocolTests = class
  public
    [Test]
    procedure EncodeDecodeLen_RoundTrip_Small;
    [Test]
    procedure EncodeDecodeLen_RoundTrip_127;
    [Test]
    procedure EncodeDecodeLen_RoundTrip_128;
    [Test]
    procedure EncodeDecodeLen_RoundTrip_16383;
    [Test]
    procedure EncodeDecodeLen_RoundTrip_2097151;
    [Test]
    procedure EncodeDecodeLen_RoundTrip_268435455;

    [Test]
    procedure EncodeDecodeStr_RoundTrip_Ascii;
    [Test]
    procedure EncodeDecodeStr_RoundTrip_UTF8;
    [Test]
    procedure EncodeDecodeStr_RoundTrip_Empty;

    [Test]
    procedure TopicMatchesFilter_Exact;
    [Test]
    procedure TopicMatchesFilter_PlusWildcard;
    [Test]
    procedure TopicMatchesFilter_HashWildcard;
    [Test]
    procedure TopicMatchesFilter_MixedWildcards;
    [Test]
    procedure TopicMatchesFilter_NoMatch;

    [Test]
    procedure IsValidTopicName_Valid;
    [Test]
    procedure IsValidTopicName_Empty_Invalid;
    [Test]
    procedure IsValidTopicName_RejectsWildcards;

    [Test]
    procedure IsValidTopicFilter_AcceptsWildcards;
    [Test]
    procedure IsValidTopicFilter_RejectsEmpty;

    [Test]
    procedure BuildConnect_311_MinimalPacket;
    [Test]
    procedure BuildConnect_5_MinimalPacket;

    [Test]
    procedure BuildPublish_QoS0_NoPacketID;
    [Test]
    procedure BuildPublish_QoS1_HasPacketID;

    [Test]
    procedure BuildParsePublish_RoundTrip_311;

    [Test]
    procedure BuildPubAck_311_Is4Bytes;
    [Test]
    procedure ParsePubAck_311_ReturnsPacketID;

    [Test]
    procedure BuildSubscribe_311_HasPayload;
    [Test]
    procedure BuildPingReq_Is2Bytes;
    [Test]
    procedure BuildDisconnect_311_Is2Bytes;

    [Test]
    procedure GetPacketType_Connect;
    [Test]
    procedure GetPacketType_Publish;
    [Test]
    procedure GetPacketType_PingReq;
  end;

implementation

{ TMQTTProtocolTests }

procedure TMQTTProtocolTests.EncodeDecodeLen_RoundTrip_Small;
var
  Encoded: TBytes;
  Offset, Decoded: Integer;
begin
  Encoded := TMQTTProtocol.EncodeLen(0);
  Assert.AreEqual<NativeInt>(1, Length(Encoded));
  Offset := 0;
  Decoded := TMQTTProtocol.DecodeLen(Encoded, Offset);
  Assert.AreEqual(0, Decoded);
end;

procedure TMQTTProtocolTests.EncodeDecodeLen_RoundTrip_127;
var
  Encoded: TBytes;
  Offset, Decoded: Integer;
begin
  Encoded := TMQTTProtocol.EncodeLen(127);
  Assert.AreEqual<NativeInt>(1, Length(Encoded));
  Offset := 0;
  Decoded := TMQTTProtocol.DecodeLen(Encoded, Offset);
  Assert.AreEqual(127, Decoded);
end;

procedure TMQTTProtocolTests.EncodeDecodeLen_RoundTrip_128;
var
  Encoded: TBytes;
  Offset, Decoded: Integer;
begin
  Encoded := TMQTTProtocol.EncodeLen(128);
  Assert.AreEqual<NativeInt>(2, Length(Encoded));
  Offset := 0;
  Decoded := TMQTTProtocol.DecodeLen(Encoded, Offset);
  Assert.AreEqual(128, Decoded);
end;

procedure TMQTTProtocolTests.EncodeDecodeLen_RoundTrip_16383;
var
  Encoded: TBytes;
  Offset, Decoded: Integer;
begin
  Encoded := TMQTTProtocol.EncodeLen(16383);
  Assert.AreEqual<NativeInt>(2, Length(Encoded));
  Offset := 0;
  Decoded := TMQTTProtocol.DecodeLen(Encoded, Offset);
  Assert.AreEqual(16383, Decoded);
end;

procedure TMQTTProtocolTests.EncodeDecodeLen_RoundTrip_2097151;
var
  Encoded: TBytes;
  Offset, Decoded: Integer;
begin
  Encoded := TMQTTProtocol.EncodeLen(2097151);
  Assert.AreEqual<NativeInt>(3, Length(Encoded));
  Offset := 0;
  Decoded := TMQTTProtocol.DecodeLen(Encoded, Offset);
  Assert.AreEqual(2097151, Decoded);
end;

procedure TMQTTProtocolTests.EncodeDecodeLen_RoundTrip_268435455;
var
  Encoded: TBytes;
  Offset, Decoded: Integer;
begin
  Encoded := TMQTTProtocol.EncodeLen(268435455);
  Assert.AreEqual<NativeInt>(4, Length(Encoded));
  Offset := 0;
  Decoded := TMQTTProtocol.DecodeLen(Encoded, Offset);
  Assert.AreEqual(268435455, Decoded);
end;

procedure TMQTTProtocolTests.EncodeDecodeStr_RoundTrip_Ascii;
var
  Encoded: TBytes;
  Offset: Integer;
  Decoded: string;
begin
  Encoded := TMQTTProtocol.EncodeStr('hello world');
  // 2 length bytes + 11 chars
  Assert.AreEqual<NativeInt>(13, Length(Encoded));
  Offset := 0;
  Decoded := TMQTTProtocol.DecodeStr(Encoded, Offset);
  Assert.AreEqual('hello world', Decoded);
end;

procedure TMQTTProtocolTests.EncodeDecodeStr_RoundTrip_UTF8;
var
  Encoded: TBytes;
  Offset: Integer;
  Decoded: string;
const
  Original = 'caffè €';
begin
  Encoded := TMQTTProtocol.EncodeStr(Original);
  Offset := 0;
  Decoded := TMQTTProtocol.DecodeStr(Encoded, Offset);
  Assert.AreEqual(Original, Decoded);
end;

procedure TMQTTProtocolTests.EncodeDecodeStr_RoundTrip_Empty;
var
  Encoded: TBytes;
  Offset: Integer;
  Decoded: string;
begin
  Encoded := TMQTTProtocol.EncodeStr('');
  Assert.AreEqual<NativeInt>(2, Length(Encoded));
  Offset := 0;
  Decoded := TMQTTProtocol.DecodeStr(Encoded, Offset);
  Assert.AreEqual('', Decoded);
end;

procedure TMQTTProtocolTests.TopicMatchesFilter_Exact;
begin
  Assert.IsTrue(TMQTTProtocol.TopicMatchesFilter('a/b/c', 'a/b/c'));
  Assert.IsFalse(TMQTTProtocol.TopicMatchesFilter('a/b/c', 'a/b/d'));
end;

procedure TMQTTProtocolTests.TopicMatchesFilter_PlusWildcard;
begin
  Assert.IsTrue(TMQTTProtocol.TopicMatchesFilter('a/b/c', 'a/+/c'));
  Assert.IsTrue(TMQTTProtocol.TopicMatchesFilter('home/kitchen/temp', 'home/+/temp'));
  Assert.IsFalse(TMQTTProtocol.TopicMatchesFilter('a/b/c/d', 'a/+/c'));
end;

procedure TMQTTProtocolTests.TopicMatchesFilter_HashWildcard;
begin
  Assert.IsTrue(TMQTTProtocol.TopicMatchesFilter('sensors/temp', 'sensors/#'));
  Assert.IsTrue(TMQTTProtocol.TopicMatchesFilter('sensors/temp/indoor', 'sensors/#'));
  Assert.IsTrue(TMQTTProtocol.TopicMatchesFilter('sensors', 'sensors/#'));
end;

procedure TMQTTProtocolTests.TopicMatchesFilter_MixedWildcards;
begin
  Assert.IsTrue(TMQTTProtocol.TopicMatchesFilter('a/b/c/d', '+/b/#'));
  Assert.IsTrue(TMQTTProtocol.TopicMatchesFilter('device1/status', '+/status/#'));
end;

procedure TMQTTProtocolTests.TopicMatchesFilter_NoMatch;
begin
  Assert.IsFalse(TMQTTProtocol.TopicMatchesFilter('home/temp', 'home/+/temp'));
  Assert.IsFalse(TMQTTProtocol.TopicMatchesFilter('x/y', 'a/#'));
end;

procedure TMQTTProtocolTests.IsValidTopicName_Valid;
begin
  Assert.IsTrue(TMQTTProtocol.IsValidTopicName('home/kitchen/temp'));
  Assert.IsTrue(TMQTTProtocol.IsValidTopicName('a'));
end;

procedure TMQTTProtocolTests.IsValidTopicName_Empty_Invalid;
begin
  Assert.IsFalse(TMQTTProtocol.IsValidTopicName(''));
end;

procedure TMQTTProtocolTests.IsValidTopicName_RejectsWildcards;
begin
  Assert.IsFalse(TMQTTProtocol.IsValidTopicName('home/+/temp'));
  Assert.IsFalse(TMQTTProtocol.IsValidTopicName('home/#'));
end;

procedure TMQTTProtocolTests.IsValidTopicFilter_AcceptsWildcards;
begin
  Assert.IsTrue(TMQTTProtocol.IsValidTopicFilter('home/+/temp'));
  Assert.IsTrue(TMQTTProtocol.IsValidTopicFilter('sensors/#'));
  Assert.IsTrue(TMQTTProtocol.IsValidTopicFilter('a/b/c'));
end;

procedure TMQTTProtocolTests.IsValidTopicFilter_RejectsEmpty;
begin
  Assert.IsFalse(TMQTTProtocol.IsValidTopicFilter(''));
end;

procedure TMQTTProtocolTests.BuildConnect_311_MinimalPacket;
var
  Opts: TMQTTConnectOptions;
  Packet: TBytes;
begin
  Opts.SetDefaults;
  Opts.ClientID := 'TestClient';
  Opts.Version := MQTT311;
  Packet := TMQTTProtocol.BuildConnect(Opts);
  Assert.IsTrue(Length(Packet) > 14);
  // Fixed header: CONNECT = 0x10
  Assert.AreEqual(Byte($10), Packet[0]);
end;

procedure TMQTTProtocolTests.BuildConnect_5_MinimalPacket;
var
  Opts: TMQTTConnectOptions;
  Packet: TBytes;
begin
  Opts.SetDefaults;
  Opts.ClientID := 'TestClient5';
  Opts.Version := MQTT5;
  Packet := TMQTTProtocol.BuildConnect(Opts);
  Assert.IsTrue(Length(Packet) > 14);
  Assert.AreEqual(Byte($10), Packet[0]);
end;

procedure TMQTTProtocolTests.BuildPublish_QoS0_NoPacketID;
var
  Packet: TBytes;
  Payload: TBytes;
begin
  Payload := TEncoding.UTF8.GetBytes('hello');
  Packet := TMQTTProtocol.BuildPublish(Ord(MQTT311), 'a/b', Payload, atMostOnce, False, False, 0);
  // First byte: type=3 << 4 = 0x30, QoS=0 means no PacketID
  Assert.AreEqual(Byte($30), Packet[0]);
end;

procedure TMQTTProtocolTests.BuildPublish_QoS1_HasPacketID;
var
  Packet: TBytes;
  Payload: TBytes;
begin
  Payload := TEncoding.UTF8.GetBytes('hello');
  Packet := TMQTTProtocol.BuildPublish(Ord(MQTT311), 'a/b', Payload, atLeastOnce, False, False, 42);
  // First byte: type=3 << 4 | QoS=1 << 1 = 0x32
  Assert.AreEqual(Byte($32), Packet[0]);
end;

procedure TMQTTProtocolTests.BuildParsePublish_RoundTrip_311;
var
  Packet, OutPayload: TBytes;
  Payload: TBytes;
  OutTopic: string;
  OutQoS: TMQTTQoS;
  OutRetain, OutDup: Boolean;
  OutPacketID: Word;
begin
  Payload := TEncoding.UTF8.GetBytes('hello world');
  Packet := TMQTTProtocol.BuildPublish(Ord(MQTT311), 'test/topic', Payload, atLeastOnce, True, False, 7);
  Assert.IsTrue(TMQTTProtocol.ParsePublish(Packet, Ord(MQTT311), OutTopic, OutPayload, OutQoS, OutRetain, OutDup, OutPacketID));
  Assert.AreEqual('test/topic', OutTopic);
  Assert.AreEqual(TEncoding.UTF8.GetString(Payload), TEncoding.UTF8.GetString(OutPayload));
  Assert.AreEqual(Ord(atLeastOnce), Ord(OutQoS));
  Assert.IsTrue(OutRetain);
  Assert.IsFalse(OutDup);
  Assert.AreEqual<Word>(7, OutPacketID);
end;

procedure TMQTTProtocolTests.BuildPubAck_311_Is4Bytes;
var
  Packet: TBytes;
begin
  Packet := TMQTTProtocol.BuildPubAck(Ord(MQTT311), 100);
  // PUBACK 3.1.1 = type 0x40 + len 2 + packetID(2) = 4 bytes
  Assert.AreEqual<NativeInt>(4, Length(Packet));
  Assert.AreEqual(Byte($40), Packet[0]);
end;

procedure TMQTTProtocolTests.ParsePubAck_311_ReturnsPacketID;
var
  Packet: TBytes;
  PacketID: Word;
  ReasonCode: Byte;
begin
  Packet := TMQTTProtocol.BuildPubAck(Ord(MQTT311), 12345);
  Assert.IsTrue(TMQTTProtocol.ParsePubAck(Packet, Ord(MQTT311), PacketID, ReasonCode));
  Assert.AreEqual<Word>(12345, PacketID);
end;

procedure TMQTTProtocolTests.BuildSubscribe_311_HasPayload;
var
  Packet: TBytes;
begin
  Packet := TMQTTProtocol.BuildSubscribe(Ord(MQTT311), 1, 'home/temp', atLeastOnce);
  // SUBSCRIBE type byte with reserved bits = 0x82
  Assert.AreEqual(Byte($82), Packet[0]);
  Assert.IsTrue(Length(Packet) > 10);
end;

procedure TMQTTProtocolTests.BuildPingReq_Is2Bytes;
var
  Packet: TBytes;
begin
  Packet := TMQTTProtocol.BuildPingReq;
  Assert.AreEqual<NativeInt>(2, Length(Packet));
  Assert.AreEqual(Byte($C0), Packet[0]);
  Assert.AreEqual(Byte($00), Packet[1]);
end;

procedure TMQTTProtocolTests.BuildDisconnect_311_Is2Bytes;
var
  Packet: TBytes;
begin
  Packet := TMQTTProtocol.BuildDisconnect(Ord(MQTT311));
  Assert.AreEqual<NativeInt>(2, Length(Packet));
  Assert.AreEqual(Byte($E0), Packet[0]);
end;

procedure TMQTTProtocolTests.GetPacketType_Connect;
var
  Packet: TBytes;
begin
  SetLength(Packet, 2);
  Packet[0] := $10;
  Packet[1] := 0;
  Assert.AreEqual(Ord(ptConnect), Ord(TMQTTProtocol.GetPacketType(Packet)));
end;

procedure TMQTTProtocolTests.GetPacketType_Publish;
var
  Packet: TBytes;
begin
  SetLength(Packet, 2);
  Packet[0] := $30;
  Packet[1] := 0;
  Assert.AreEqual(Ord(ptPublish), Ord(TMQTTProtocol.GetPacketType(Packet)));
end;

procedure TMQTTProtocolTests.GetPacketType_PingReq;
var
  Packet: TBytes;
begin
  Packet := TMQTTProtocol.BuildPingReq;
  Assert.AreEqual(Ord(ptPingReq), Ord(TMQTTProtocol.GetPacketType(Packet)));
end;

initialization
  TDUnitX.RegisterTestFixture(TMQTTProtocolTests);

end.
