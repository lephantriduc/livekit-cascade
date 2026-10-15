# Handoff guide: cascading SFU-to-SFU

## Mục đích của tài liệu

Tài liệu này dành cho developer tiếp nhận và tiếp tục phát triển phần
cascading trong repo LiveKit này. Mục tiêu không chỉ là liệt kê file, mà là
giải thích:

- feature được khởi tạo và chạy qua những lớp nào;
- control plane và media plane giao tiếp ra sao;
- ownership/lifecycle của từng object;
- các contract cần giữ khi sửa code;
- cách chạy test, demo và debug;
- phần nào đã có code nhưng chưa được wire thành hành vi hoàn chỉnh.

Phạm vi:

- toàn bộ 18 file trong `pkg/cascading/`, kể cả file không bị sửa trong hai
  commit;
- toàn bộ 15 file ngoài package được hai commit `a08d5180` và `32d13f69`
  chạm tới.

## 1. Bắt đầu đọc code từ đâu

Nếu cần hiểu nhanh hệ thống, đọc theo thứ tự này:

1. `pkg/service/cascade.go`: composition root, validation, topology và
   lifecycle runtime.
2. `pkg/service/server.go`: nơi runtime được tạo, route được đăng ký,
   start/close.
3. `pkg/cascading/link_manager.go`: cách hai SFU tạo và nhận WebRTC link.
4. `pkg/cascading/link.go`: data channel control và track sender.
5. `pkg/cascading/gossip.go` và `manager.go`: announcement control plane.
6. `pkg/cascading/relay.go`, `cascade_sender.go`, `cascade_receiver.go`: media
   plane.
7. `pkg/rtc/room.go` và `pkg/service/roommanager.go`: điểm móc vào room.
8. `pkg/cascading/topology/topology.go`: cách chọn neighbor.
9. `pkg/telemetry/prometheus/cascade.go`: metric quan sát.

Hai commit tạo feature:

- `a08d5180`: thêm config, message/manager và hook vào room.
- `32d13f69`: thêm runtime wiring, link/gossip/relay/election integration,
  metrics và demo.

## 2. Mental model: một node cascading gồm những gì?

Mỗi SFU có một `cascadeRuntime` gồm:

```text
cascadeRuntime
├── LinkManager       map peer ID -> SFULink
├── RelayManager      map track ID -> peer ID -> CascadeSender
├── Manager           announce/receive track-published gossip
├── peers             peer ID -> SFUPeerInfo
└── neighbors         peer IDs do topology strategy chọn
```

Hai mặt phẳng cần phân biệt:

### Control plane

```text
Room.onTrackPublished
  -> Manager.AnnounceTrackPublished
  -> MeshGossipEngine.Broadcast
  -> SFULink.SendGossip
  -> ordered WebRTC DataChannel "control"
  -> MeshGossipEngine.HandleIncoming
  -> Manager.OnGossipTrackPublished
  -> callback OnTrackPublished
```

Gossip chỉ chuyển metadata JSON. Nó không tự chuyển RTP và không tự tạo
subscription/relay.

### Media plane

```text
local TrackReceiver
  -> RelayManager.RelayTrack
  -> CascadeSender (downtrack)
  -> TrackLocalStaticRTP của SFULink
  -> RTPSender trên PeerConnection
  -> remote TrackRemote
  -> CascadeReceiver
  -> sfu.ReceiverBase/buffer
```

Hiện code cung cấp các building block của media plane, nhưng callback nhận
announcement chưa tự động gọi `RelayTrack`.

## 3. Lifecycle của feature

### 3.1. Khởi tạo server

`NewLivekitServer` gọi `newCascadeRuntime(conf)`:

1. Nếu `cascade.enabled` là false, trả `nil, nil`; server giữ nguyên hành vi
   non-cascading.
2. Yêu cầu `self_id` và danh sách peer không rỗng.
3. Validate mọi peer có `id` và `addr`, loại self khỏi map, phát hiện duplicate.
4. Chọn topology.
5. Tạo `HTTPSignaling`, `LinkManager`, `MeshGossipEngine`,
   `RelayManager` và `Manager`.
6. Gắn `Manager` vào `RoomManager`, để room mới nhận cùng manager.

### 3.2. Đăng ký HTTP

Khi runtime tồn tại, server đăng ký:

- `POST /cascade/signal`: nhận SDP offer từ SFU khác;
- `/debug/cascade` khi `development: true`: trả self ID, active links, relay
  count và coordinator.

Signaling hiện chạy trên HTTP server chính. `listen_port` không tạo listener
riêng.

### 3.3. Start link

Sau khi server bắt đầu serve, `cascadeRuntime.start()` chạy goroutine:

1. lặp qua `neighbors`;
2. gọi `EnsureLink` với timeout 10 giây;
3. log warning và tiếp tục peer kế tiếp nếu một link lỗi.

Trong `EnsureLink`, node có ID nhỏ hơn là offerer; node có ID lớn hơn chỉ
answer incoming offer. Đây là quy ước tránh hai phía đồng thời tạo offer.

### 3.4. Shutdown

Server gọi `cascadeRuntime.close()`, duyệt tất cả `SFULink` và đóng
`PeerConnection`. Relay sender sẽ được giải phóng khi link đóng/relay bị stop.

## 4. Contract và data model trong `pkg/cascading/`

### 4.1. `messages.go`: protocol nội bộ

Các kiểu chính:

- `SFUPeerInfo`: `NodeID`, `IP`, `Addr`, map `PingMs`.
- `TrackRelayInfo`: track và participant liên quan.
- `CascadeControlMsg`: envelope gửi trên data channel, gồm `Type`, JSON
  `Payload`, `Version`.
- `GossipMsg`: `MsgID`, `SenderID`, `TTL`, payload.
- `ElectionMsg`, `AnswerMsg`, `CoordinatorMsg`: election protocol.
- `TrackPublishedGossip`: `OriginSFUID`, optional `DestSFUID`, `Track`.

Các constant control hiện có:
`RELAY_TRACK`, `STOP_RELAY`, `SUBSCRIPTION_REQUEST`, `PLI_REQUEST`,
`GOSSIP`.

`IHaveMsg`, `GraftMsg`, `PruneMsg` là kiểu dữ liệu chuẩn bị cho Plumtree,
chưa có engine tương ứng.

Khi thêm message mới, phải giữ envelope versioning và không gửi object Go
trực tiếp: payload được JSON encode thành `[]byte`.

### 4.2. `link.go`: một kết nối SFU-to-SFU

`SFULink` sở hữu:

- `PeerConnection`;
- negotiated ordered data channel `control`, ID `0`;
- callback control và callback gossip.

API quan trọng:

- `SendControl`: JSON encode `CascadeControlMsg`; lỗi nếu data channel chưa
  init.
- `SendGossip`: bọc `GossipMsg` thành control message type `GOSSIP`.
- `InitControlChannel`: tạo channel và phân loại message nhận được.
- `AddTrack`/`RemoveTrack`: thêm hoặc gỡ RTP sender khỏi peer connection.
- `Close`: đóng peer connection.

JSON lỗi trên data channel hiện bị bỏ qua. Nếu cần observability hoặc protocol
error response, phải bổ sung có chủ đích, không giả vờ xử lý thành công.

### 4.3. `signaling.go`: SDP transport

`HTTPSignaling.SendOffer` POST offer tới:

```text
http://<dstAddr>/cascade/signal
```

Headers:

- `Content-Type: application/sdp`;
- `Node-ID: <source SFU ID>`.

Implementation hiện dùng HTTP client mặc định, chưa TLS/auth riêng, chưa
kiểm tra HTTP status và chưa xử lý response body theo lifecycle production.
Đây là điểm cần sửa trước khi expose ngoài mạng tin cậy.

### 4.4. `link_manager.go`: ownership của link

`LinkManager` là owner của map `remoteID -> *SFULink` và là nơi duy nhất tạo
link trong runtime.

`EnsureLink`:

- trả link hiện có nếu peer đã tồn tại;
- tạo `PeerConnection`, lưu link và notify `OnLink`;
- nếu local ID nhỏ hơn remote ID, tạo offer, gather ICE, gửi offer và set
  answer;
- nếu setup offer thất bại, xóa map entry, close peer connection và trả lỗi.

`RegisterHTTPHandler` xử lý offer incoming, tạo link nếu cần, init channel,
set remote description, create/set answer và trả SDP.

`OnLink` callback được gọi cả cho link mới và mọi link đã tồn tại tại thời điểm
đăng ký. Gossip engine dựa vào contract này để gắn handler cho từng link.

### 4.5. `gossip.go`: fan-out và chống loop

`GossipEngine` chỉ yêu cầu:

```go
Broadcast(payload []byte) error
OnReceive(func(senderID string, payload []byte))
```

`MeshGossipEngine`:

- tạo UUID `MsgID`;
- mark-seen trước khi gửi;
- gửi tới tất cả link;
- khi nhận message mới, gọi callback local;
- forward tới mọi link trừ sender;
- bỏ qua message có `MsgID` đã thấy;
- dùng LRU tối đa 10.000 message ID.

`Broadcast` trả lỗi đầu tiên nhưng vẫn cố gửi các link còn lại. Khi xử lý lỗi
ở caller, không được giả định broadcast atomic.

### 4.6. `manager.go`: announcement facade

`Manager` là facade giữa room lifecycle và gossip:

- `AnnounceTrackPublished(track)` serialize `TrackPublishedGossip` và broadcast;
- `OnTrackPublished(fn)` lưu callback;
- payload nhận từ gossip được unmarshal rồi đưa vào callback;
- bỏ qua message từ chính origin hoặc thiếu track.

Manager không biết room nào sở hữu track, không chọn destination, không tạo
relay. Developer tích hợp logic subscription phải làm rõ ownership của room,
track và relay trước khi gọi callback này.

### 4.7. `relay.go`: relay lifecycle

`RelayManager.relays` có key hai tầng:

```text
relays[trackID][destinationPeerID] = *CascadeSender
```

`RelayTrack(trackID, dstPeerID, localReceiver)`:

1. yêu cầu link đã tồn tại;
2. dùng codec receiver để add track vào link;
3. tạo `CascadeSender`;
4. add sender làm downtrack của receiver;
5. thay relay cũ cùng `(track, peer)`;
6. tăng gauge active relay nếu là relay mới.

Nếu add downtrack lỗi, sender/link track được rollback. `StopRelay` remove
map, close sender và giảm gauge. `ActiveCount` phục vụ debug.

### 4.8. `cascade_sender.go`: adapter sang `sfu.TrackSender`

`CascadeSender.WriteRTP` ghi raw packet vào `TrackLocalStaticRTP`. Nó trả
remote peer như `SubscriberID`, track ID như `ID`, và remove RTPSender khi
`Close`.

Các callback bitrate/layer/resync hiện là no-op có chủ ý vì đây là đường
relay giữa SFU, không phải subscriber Internet thông thường. `IsClosed`
hiện luôn trả false; nếu lifecycle code cần dùng trạng thái đóng, đây là
technical debt cần xử lý cùng test cạnh tranh.

### 4.9. `cascade_receiver.go`: adapter từ remote RTP

`NewCascadeReceiver` bọc `webrtc.TrackRemote` bằng `sfu.ReceiverBase`,
bind codec vào cascade buffer và start layer 0. `Start` đọc RTP trong loop,
unmarshal packet rồi đưa vào buffer với monotonic timestamp.

- lỗi đọc remote track kết thúc receiver;
- packet không parse được bị bỏ qua;
- `SendPLI` chỉ gọi callback nếu có.

Đây là nơi cần bổ sung xử lý PLI/control nếu relay cần keyframe recovery.

### 4.10. `registry.go`: peer discovery abstraction

`PeerRegistry` có `Discover` và `Watch`. `StaticPeerRegistry` trả danh sách
được truyền lúc khởi tạo; `Watch` trả channel đóng ngay vì không có dynamic
membership. Runtime hiện đọc peer từ config trực tiếp, chưa dùng registry.

Nếu triển khai dynamic discovery, phải quyết định rõ behavior khi peer rời:
close link, stop relay, cập nhật topology và xử lý message đang in-flight.

### 4.11. `topology/topology.go`: neighbor selection

`Strategy.Neighbors(selfID, allNodes)` có các implementation:

- `Mesh`: mọi node khác;
- `HubSpoke`: hub nối mọi spoke, spoke chỉ nối hub;
- `Ring`: node trước/sau theo thứ tự danh sách;
- `Line`: tối đa node trước/sau;
- `Custom`: edge khai báo, lọc self/unknown/duplicate.

Parser trong `pkg/service/cascade.go` chỉ expose mesh, hub-spoke,
ring và line. `Custom` chưa cấu hình được dù implementation đã tồn tại.

## 5. Integration với LiveKit

### 5.1. `pkg/config/config.go` và `config-sample.yaml`

Root config thêm:

```yaml
cascade:
  enabled: false
  self_id: sfu-a
  self_addr: 127.0.0.1:7882
  topology: mesh
  hub_id: ""
  listen_port: 7882
  peers:
    - id: sfu-b
      addr: 127.0.0.1:7883
```

Ý nghĩa:

- `enabled`: feature flag;
- `self_id`: identity dùng trong signaling/election/gossip;
- `self_addr`: địa chỉ quảng bá, hiện chưa được runtime dùng;
- `topology`, `hub_id`: chọn neighbor;
- `listen_port`: field chuẩn bị cho listener riêng, hiện chưa được dùng;
- `peers`: ID và địa chỉ signaling của các SFU.

`config_test.go` kiểm tra YAML mapping của block cascade.

### 5.2. `pkg/service/cascade.go`

Đây là composition root. Validation errors cần được giữ rõ ràng vì server
không nên start với cascade config sai:

- thiếu `self_id`;
- thiếu peers;
- peer thiếu ID/address;
- duplicate peer ID;
- topology không hỗ trợ;
- hub-spoke thiếu `hub_id`.

`start` dùng timeout 10 giây cho từng neighbor và không fail-fast toàn server.
Đây là behavior cần giữ khi cải tiến retry/backoff.

### 5.3. `pkg/service/server.go`

Server:

1. tạo runtime;
2. đưa manager vào `RoomManager`;
3. register signaling;
4. register debug endpoint trong development;
5. start link setup sau khi listener chạy;
6. close links trong shutdown.

Khi thêm transport hoặc manager mới, phải nối đủ cả ba pha: construction,
registration/start và shutdown.

### 5.4. `pkg/service/roommanager.go`

RoomManager giữ một `*cascading.Manager` dùng chung. Khi tạo room, manager
được truyền vào room mới. Setter và access được bảo vệ bằng lock.

### 5.5. `pkg/rtc/room.go`

Room có `cascadeManager` và `SetCascadeManager`.

Trong `onTrackPublished`:

1. track được add vào local track manager;
2. `AnnounceTrackPublished(track.ToProto())` được gọi;
3. lỗi announce chỉ log warning, không làm fail publish local.

`AddRelayedTrack` add track nhận từ SFU khác nhưng không announce lại, tránh
gossip loop. Khi nối logic receiver vào room, phải giữ invariant này.

## 6. Election: có code nhưng chưa chạy trong runtime

`BullyElection` dùng score:

```text
availableBandwidth / (1 + pingMs + 100 * cpuLoad)
```

Score cao hơn thắng. Election dùng epoch từ `EpochStore`, gửi `ElectionMsg`,
đợi answer tối đa 3 giây, rồi broadcast `CoordinatorMsg` nếu không có answer.
Epoch cũ bị bỏ qua; election metric tăng khi bắt đầu.

Quan trọng khi tiếp nhận code: `cascadeRuntime` hiện **chưa tạo hoặc wire
`BullyElection`**, chưa có transport thực tế cho election và debug handler
nhận `nil` election. Do đó không được mô tả current hub như một chức năng đã
hoạt động chỉ vì type/logic đã tồn tại.

## 7. Metrics và debug

`pkg/telemetry/prometheus/cascade.go` khai báo:

| Metric | Ý nghĩa |
|---|---|
| `cascade_relay_tracks_active{from_sfu,to_sfu}` | relay đang active |
| `cascade_gossip_sent_total{peer}` | gossip gửi thành công |
| `cascade_link_rtt_ms{peer}` | thời gian setup link |
| `cascade_elections_total` | số lần bắt đầu election |

`node.go` gọi `initCascadeStats()` trong node metric initialization. Collector
được register một lần trong lifecycle đó.

Khi debug local:

- gọi `/debug/cascade` để xem `self_id`, `active_links`, `relay_tracks`;
- xem Prometheus để đối chiếu link/gossip/relay;
- log `cascade link setup failed`, `cascade link established`,
  `cascade relay started` và election result.

## 8. Demo và cách chạy

Các file:

- `demo/config-sfu-a.yaml`, `config-sfu-b.yaml`, `config-sfu-c.yaml`;
- `demo/prometheus.yml`;
- `docker-compose.yml`.

Compose chạy Redis, ba LiveKit SFU, Prometheus và Grafana trên network
`sfunet`. Các SFU gọi nhau bằng service DNS `sfu-a/b/c:7880`, trong khi host
port được map riêng:

| Service | HTTP | Metrics | Debug |
|---|---:|---:|---:|
| sfu-a | 7880 | 6789 | 7070 |
| sfu-b | 7881 | 6790 | 7071 |
| sfu-c | 7882 | 6791 | 7072 |

Prometheus chạy host port 9090, Grafana host port 3000. Password Grafana
`admin` chỉ dành cho demo.

Lệnh chạy:

```bash
docker compose up --build
```

Trong container, không dùng host port 7881/7882 để peer-to-peer signaling:
config đúng là `sfu-b:7880` và `sfu-c:7880`.

## 9. Test map và workflow sửa code

Chạy targeted suite:

```bash
go test ./pkg/cascading/... ./pkg/config/... ./pkg/service/... ./pkg/rtc/...
```

Test hiện có:

- `cascade_sender_test.go`: raw RTP qua `CascadeSender`;
- `relay_smoke_test.go`: RTP end-to-end qua `CascadeReceiver`;
- `link_integration_test.go`: SDP signaling và control data channel;
- `election_test.go`: score, epoch và higher-score response;
- `registry_test.go`: static discovery;
- `topology/topology_test.go`: mọi topology;
- `config_test.go`: YAML cascade mapping;
- `pkg/service/cascade_test.go`: runtime creation và validation.

Khi thay đổi:

1. sửa protocol/message trước nếu contract wire thay đổi;
2. cập nhật link/control handler nếu thêm message;
3. cập nhật lifecycle ở `cascadeRuntime` và server;
4. cập nhật room hook nếu behavior track thay đổi;
5. thêm test unit rồi test integration RTP/signaling;
6. kiểm tra metric increment/decrement cân bằng;
7. kiểm tra shutdown và duplicate link/relay.

## 10. Danh sách file đầy đủ

### 10.1. `pkg/cascading/` (18 file)

`cascade_receiver.go`, `cascade_sender.go`, `cascade_sender_test.go`,
`election.go`, `election_test.go`, `gossip.go`, `link.go`,
`link_integration_test.go`, `link_manager.go`, `manager.go`, `messages.go`,
`registry.go`, `registry_test.go`, `relay.go`, `relay_smoke_test.go`,
`signaling.go`, `topology/topology.go`, `topology/topology_test.go`.

### 10.2. Ngoài `pkg/cascading/` (15 file)

`config-sample.yaml`, `docker-compose.yml`,
`demo/config-sfu-a.yaml`, `demo/config-sfu-b.yaml`,
`demo/config-sfu-c.yaml`, `demo/prometheus.yml`,
`pkg/config/config.go`, `pkg/config/config_test.go`, `pkg/rtc/room.go`,
`pkg/service/cascade.go`, `pkg/service/cascade_test.go`,
`pkg/service/roommanager.go`, `pkg/service/server.go`,
`pkg/telemetry/prometheus/cascade.go`,
`pkg/telemetry/prometheus/node.go`.

## 11. Known gaps trước khi gọi là production-ready

1. Signaling HTTP chưa có TLS/auth riêng, chưa kiểm tra status và chưa đóng
   response body trong `HTTPSignaling`.
2. `self_addr` và `listen_port` chưa có tác dụng runtime.
3. Topology chỉ chọn link neighbor; gossip engine vẫn là mesh fan-out trên
   các link đã tạo.
4. `Custom` topology chưa được expose qua config parser.
5. Election chưa được tạo/wire vào runtime.
6. `OnTrackPublished` chưa ánh xạ announcement sang room/participant và
   `RelayManager.RelayTrack`.
7. `AddRelayedTrack` là API nhưng chưa thấy end-to-end integration tự động từ
   remote gossip tới room.
8. `CascadeSender.IsClosed()` luôn false.
9. Dynamic peer registry chưa được dùng; peer list là static startup config.
10. Demo dùng secret/key/password mặc định, không dùng cho production.

Khi tiếp tục phát triển, nên ưu tiên security của signaling, sau đó hoàn thiện
track routing/relay lifecycle, rồi mới wire dynamic discovery và election.
