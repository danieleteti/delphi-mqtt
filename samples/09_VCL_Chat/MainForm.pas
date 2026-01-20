unit MainForm;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants,
  System.Classes, Vcl.Graphics, Vcl.Controls, Vcl.Forms, Vcl.Dialogs,
  Vcl.StdCtrls, Vcl.ExtCtrls, Vcl.ComCtrls,
  MQTT.Types, MQTT.Client;

type
  TfrmMain = class(TForm)
    pnlTop: TPanel;
    pnlBottom: TPanel;
    pnlMessages: TPanel;
    lblHost: TLabel;
    edtHost: TEdit;
    lblPort: TLabel;
    edtPort: TEdit;
    lblNickname: TLabel;
    edtNickname: TEdit;
    btnConnect: TButton;
    btnDisconnect: TButton;
    lblRoom: TLabel;
    edtRoom: TEdit;
    btnJoin: TButton;
    btnLeave: TButton;
    memoMessages: TMemo;
    lblMessage: TLabel;
    edtMessage: TEdit;
    btnSend: TButton;
    StatusBar: TStatusBar;
    lblOnline: TLabel;
    lstOnline: TListBox;
    pnlRight: TPanel;
    Splitter1: TSplitter;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure btnConnectClick(Sender: TObject);
    procedure btnDisconnectClick(Sender: TObject);
    procedure btnJoinClick(Sender: TObject);
    procedure btnLeaveClick(Sender: TObject);
    procedure btnSendClick(Sender: TObject);
    procedure edtMessageKeyPress(Sender: TObject; var Key: Char);
    procedure FormCloseQuery(Sender: TObject; var CanClose: Boolean);
  private
    FClient: IMQTTClient;
    FNickname: string;
    FRoom: string;
    FConnected: Boolean;
    FJoined: Boolean;
    procedure UpdateUI;
    procedure Log(const Msg: string);
    procedure HandleChatMessage(const Topic: string; const Payload: TBytes);
    procedure HandlePresence(const Topic: string; const Payload: TBytes);
    procedure HandleDisconnect(ReasonCode: TMQTTReasonCode; const ReasonString: string);
    procedure HandleError(const ErrorMsg: string);
    procedure AnnouncePresence(Online: Boolean);
    procedure UpdatePresenceUI(const User: string; Online: Boolean);
  public
  end;

var
  frmMain: TfrmMain;

implementation

{$R *.dfm}

uses
  System.JSON;

procedure TfrmMain.FormCreate(Sender: TObject);
begin
  FClient := CreateMQTTClient;
  FConnected := False;
  FJoined := False;

  // Generate random nickname
  Randomize;
  edtNickname.Text := 'User' + IntToStr(Random(9999));

  // Set event handlers
  FClient.SetOnDisconnect(HandleDisconnect);
  FClient.SetOnError(HandleError);

  UpdateUI;
end;

procedure TfrmMain.FormDestroy(Sender: TObject);
begin
  if FJoined then
    AnnouncePresence(False);
  if FConnected then
    FClient.Disconnect;
end;

procedure TfrmMain.FormCloseQuery(Sender: TObject; var CanClose: Boolean);
begin
  if FJoined then
    AnnouncePresence(False);
  if FConnected then
  begin
    FClient.Disconnect;
    Sleep(200); // Give time to send disconnect
  end;
  CanClose := True;
end;

procedure TfrmMain.UpdateUI;
begin
  // Connection controls
  edtHost.Enabled := not FConnected;
  edtPort.Enabled := not FConnected;
  edtNickname.Enabled := not FConnected;
  btnConnect.Enabled := not FConnected;
  btnDisconnect.Enabled := FConnected;

  // Room controls
  edtRoom.Enabled := FConnected and not FJoined;
  btnJoin.Enabled := FConnected and not FJoined;
  btnLeave.Enabled := FJoined;

  // Chat controls
  edtMessage.Enabled := FJoined;
  btnSend.Enabled := FJoined;

  // Status
  if FJoined then
    StatusBar.SimpleText := Format('Connected as "%s" in room "%s"', [FNickname, FRoom])
  else if FConnected then
    StatusBar.SimpleText := 'Connected - Join a room to start chatting'
  else
    StatusBar.SimpleText := 'Disconnected';
end;

procedure TfrmMain.Log(const Msg: string);
begin
  TThread.Queue(nil, procedure
  begin
    memoMessages.Lines.Add('[' + FormatDateTime('hh:nn:ss', Now) + '] ' + Msg);
    // Auto-scroll to bottom
    SendMessage(memoMessages.Handle, WM_VSCROLL, SB_BOTTOM, 0);
  end);
end;

procedure TfrmMain.btnConnectClick(Sender: TObject);
var
  Options: TMQTTConnectOptions;
begin
  FNickname := Trim(edtNickname.Text);
  if FNickname = '' then
  begin
    ShowMessage('Please enter a nickname');
    edtNickname.SetFocus;
    Exit;
  end;

  try
    Options.SetDefaults;
    Options.ClientID := 'MQTTChat_' + FNickname + '_' + IntToStr(Random(10000));
    Options.KeepAliveSec := 30;
    Options.CleanStart := True;

    FClient.Connect(edtHost.Text, StrToIntDef(edtPort.Text, 1883), Options);
    FConnected := True;
    Log('Connected to ' + edtHost.Text);
    UpdateUI;
  except
    on E: Exception do
    begin
      Log('Connection failed: ' + E.Message);
      ShowMessage('Connection failed: ' + E.Message);
    end;
  end;
end;

procedure TfrmMain.btnDisconnectClick(Sender: TObject);
begin
  if FJoined then
  begin
    AnnouncePresence(False);
    FJoined := False;
  end;

  FClient.Disconnect;
  FConnected := False;
  lstOnline.Clear;
  Log('Disconnected');
  UpdateUI;
end;

procedure TfrmMain.btnJoinClick(Sender: TObject);
var
  RoomTopic: string;
begin
  FRoom := Trim(edtRoom.Text);
  if FRoom = '' then
  begin
    ShowMessage('Please enter a room name');
    edtRoom.SetFocus;
    Exit;
  end;

  RoomTopic := 'chat/' + FRoom;

  // Subscribe to chat messages
  FClient.Subscribe(RoomTopic + '/messages', HandleChatMessage, atLeastOnce);

  // Subscribe to presence announcements
  FClient.Subscribe(RoomTopic + '/presence', HandlePresence, atLeastOnce);

  FJoined := True;
  lstOnline.Clear;
  lstOnline.Items.Add(FNickname + ' (me)');

  Log('Joined room "' + FRoom + '"');

  // Announce presence
  AnnouncePresence(True);

  UpdateUI;
  edtMessage.SetFocus;
end;

procedure TfrmMain.btnLeaveClick(Sender: TObject);
var
  RoomTopic: string;
begin
  RoomTopic := 'chat/' + FRoom;

  // Announce leaving
  AnnouncePresence(False);

  // Unsubscribe
  FClient.Unsubscribe(RoomTopic + '/messages');
  FClient.Unsubscribe(RoomTopic + '/presence');

  Log('Left room "' + FRoom + '"');

  FJoined := False;
  FRoom := '';
  lstOnline.Clear;
  UpdateUI;
end;

procedure TfrmMain.btnSendClick(Sender: TObject);
var
  RoomTopic: string;
  JSON: TJSONObject;
  Msg: string;
begin
  Msg := Trim(edtMessage.Text);
  if Msg = '' then
    Exit;

  RoomTopic := 'chat/' + FRoom;

  // Create JSON message
  JSON := TJSONObject.Create;
  try
    JSON.AddPair('from', FNickname);
    JSON.AddPair('text', Msg);
    JSON.AddPair('time', FormatDateTime('yyyy-mm-dd"T"hh:nn:ss', Now));

    FClient.Publish(RoomTopic + '/messages', JSON.ToString, atLeastOnce);
  finally
    JSON.Free;
  end;

  edtMessage.Clear;
  edtMessage.SetFocus;
end;

procedure TfrmMain.edtMessageKeyPress(Sender: TObject; var Key: Char);
begin
  if Key = #13 then
  begin
    Key := #0;
    btnSendClick(nil);
  end;
end;

procedure TfrmMain.HandleChatMessage(const Topic: string; const Payload: TBytes);
var
  JSON: TJSONObject;
  FromUser, Text: string;
begin
  try
    JSON := TJSONObject.ParseJSONValue(TEncoding.UTF8.GetString(Payload)) as TJSONObject;
    if JSON <> nil then
    try
      FromUser := JSON.GetValue<string>('from', '???');
      Text := JSON.GetValue<string>('text', '');
      Log(FromUser + ': ' + Text);
    finally
      JSON.Free;
    end;
  except
    // Ignore malformed messages
  end;
end;

procedure TfrmMain.HandlePresence(const Topic: string; const Payload: TBytes);
var
  JSON: TJSONObject;
  User, Status: string;
  Online, IsNew: Boolean;
  Idx: Integer;
begin
  try
    JSON := TJSONObject.ParseJSONValue(TEncoding.UTF8.GetString(Payload)) as TJSONObject;
    if JSON <> nil then
    try
      User := JSON.GetValue<string>('user', '');
      Status := JSON.GetValue<string>('status', '');
      Online := (Status = 'online');

      if User = FNickname then
        Exit; // Ignore own presence

      // Check if this is a new user joining
      Idx := lstOnline.Items.IndexOf(User);
      IsNew := (Idx < 0) and Online;

      UpdatePresenceUI(User, Online);

      // If someone new joined, re-announce our presence so they see us
      if IsNew then
        AnnouncePresence(True);
    finally
      JSON.Free;
    end;
  except
    // Ignore malformed messages
  end;
end;

procedure TfrmMain.HandleDisconnect(ReasonCode: TMQTTReasonCode; const ReasonString: string);
var
  LocalReason: string;
begin
  LocalReason := ReasonString;
  TThread.Queue(nil, procedure
  begin
    FConnected := False;
    FJoined := False;
    lstOnline.Clear;
    Log('Disconnected: ' + LocalReason);
    UpdateUI;
  end);
end;

procedure TfrmMain.HandleError(const ErrorMsg: string);
begin
  Log('Error: ' + ErrorMsg);
end;

procedure TfrmMain.AnnouncePresence(Online: Boolean);
var
  RoomTopic: string;
  JSON: TJSONObject;
begin
  if FRoom = '' then
    Exit;

  RoomTopic := 'chat/' + FRoom;

  JSON := TJSONObject.Create;
  try
    JSON.AddPair('user', FNickname);
    if Online then
      JSON.AddPair('status', 'online')
    else
      JSON.AddPair('status', 'offline');

    FClient.Publish(RoomTopic + '/presence', JSON.ToString, atLeastOnce);
  finally
    JSON.Free;
  end;
end;

procedure TfrmMain.UpdatePresenceUI(const User: string; Online: Boolean);
var
  LocalUser: string;
  LocalOnline: Boolean;
begin
  // Capture values for thread safety
  LocalUser := User;
  LocalOnline := Online;

  TThread.Queue(nil, procedure
  var
    Idx: Integer;
  begin
    Idx := lstOnline.Items.IndexOf(LocalUser);

    if LocalOnline then
    begin
      if Idx < 0 then
      begin
        lstOnline.Items.Add(LocalUser);
        Log('>> ' + LocalUser + ' joined the room');
      end;
    end
    else
    begin
      if Idx >= 0 then
      begin
        lstOnline.Items.Delete(Idx);
        Log('<< ' + LocalUser + ' left the room');
      end;
    end;
  end);
end;

end.
