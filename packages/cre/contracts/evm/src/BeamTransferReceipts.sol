// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {ReceiverTemplate} from "./vendor/chainlink/ReceiverTemplate.sol";

/// @title BeamTransferReceipts
/// @notice Onchain receipts for finished Beam transfers and Room deliveries, plus the bounded set of open
///         requests their writer answers. The writer is fixed at deployment (`donAttested`):
///         - a Chainlink CRE workflow, whose DON-signed reports arrive through a Chainlink forwarder;
///         - a relay, on chains CRE does not serve, which delivers reports itself as the forwarder account.
/// @dev Every receipt carries an evidence level, a bitmask of what the receipt can claim:
///      - L1, Beam-reported (`LEVEL_BEAM_REPORTED`): Beam's API reported this status, size and plan
///        fingerprint. It does not show that bytes arrived intact, and it relies on Beam's honesty.
///      - L2, target-signed receipt (`LEVEL_TARGET_SIGNED`, Room receipts only): the evidence committed by
///        `evidenceSha256` holds, for every target, a receipt signed by the agent holding receipt key K over
///        `manifestSha256`. It does not show that K belongs to member M (that binding is Beam-attested), nor
///        that the content is correct; for end-to-end encrypted Rooms the hashes are over ciphertext. Neither
///        this contract nor the workflow checks those signatures; anyone can, against the evidence.
///      - L3, DON-signed report (`LEVEL_DON_ATTESTED`, CRE deployments only): enough CRE nodes fetched Beam's
///        evidence and got the same answer, and this write is authentic. Beam's evidence itself is not
///        independently authenticated: a wrong Beam answer would be attested faithfully.
///      Standard receipts are L1, plus L3 in a CRE deployment. Room receipts are the same, plus L2 when every
///      target returned a signed receipt. A relay deployment never records L3: its receipts rest on the relay
///      operator reporting Beam's answer. A job result links the Room receipts of a job's input and output, and
///      carries only the claims both hold. Everything stored or emitted here is public onchain metadata;
///      payloads never are.
contract BeamTransferReceipts is ReceiverTemplate {
  /// @notice Version of the report layout `_processReport` decodes.
  uint8 public constant REPORT_VERSION = 2;

  /// @notice Closure reason: Beam answered `409 bucket_only_publication` for a Room receipt request. The
  ///         publication ran as a standard transfer and has no Room receipt; its `transferId` is the one to request.
  uint8 public constant CLOSURE_BUCKET_ONLY_PUBLICATION = 1;

  uint8 public constant LEVEL_BEAM_REPORTED = 0x01;
  uint8 public constant LEVEL_TARGET_SIGNED = 0x02;
  uint8 public constant LEVEL_DON_ATTESTED = 0x04;

  /// @notice Upper bound of the open-request set. A request beyond it reverts until one is answered or cancelled.
  uint256 public constant MAX_OPEN_REQUESTS = 256;
  /// @notice Upper bound of `requestTransfer` input, which keeps the request log inside CRE's log-event size limit.
  uint256 public constant MAX_INPUT_BYTES = 2048;
  /// @notice Upper bound of each Room, channel and publication id.
  uint256 public constant MAX_ROOM_ID_BYTES = 128;

  enum Status {
    None,
    Completed,
    Failed,
    Cancelled,
    Partial
  }

  enum RequestKind {
    None,
    Transfer,
    Receipt,
    RoomReceipt
  }

  /// @notice What a `requestTransfer` asks for:
  ///         - `Standard`: a transfer, whose id derives from the request id;
  ///         - `RoomDelivery`: a file delivered to Room members, published under the request's key;
  ///         - `Job`: a Room delivery of a job's input to a compute member, answered once the compute member's output
  ///           is delivered too, by a job result linking both Room receipts.
  enum TransferKind {
    None,
    Standard,
    RoomDelivery,
    Job
  }

  /// @notice A standard transfer receipt, keyed by the transfer UUID.
  /// @dev `requestId` is the `requestTransfer` id this transfer answers, or zero.
  ///      `completedAtMs` is Beam's completion event time in Unix milliseconds.
  ///      `planFingerprint` and `coordinateChecksum` are zero when Beam reported none.
  struct StandardReceipt {
    bytes32 requestId;
    bytes32 planFingerprint;
    bytes32 coordinateChecksum;
    uint64 totalBytes;
    uint64 bytesTransferred;
    uint64 completedAtMs;
    Status status;
    uint8 level;
  }

  /// @notice A Room delivery receipt, keyed by `roomReceiptKey(room, channel, publication)`.
  /// @dev `evidenceSha256` commits to the exact bytes of Beam's receipt evidence envelope, and
  ///      `assignmentsSha256` is Beam's commitment to the ordered assignment list (L1).
  struct RoomReceipt {
    bytes32 manifestSha256;
    bytes32 evidenceSha256;
    bytes32 assignmentsSha256;
    uint64 fileSizeBytes;
    uint32 targetCount;
    uint16 evidenceVersion;
    Status status;
    uint8 level;
  }

  /// @notice The result of a job: the Room receipts of its input and output, and what they hold in common.
  /// @dev `status` is the output's when the input completed, and the input's otherwise. `level` holds the claims
  ///      both receipts carry. `computeMember` is the Room member that published the output; that it runs on the
  ///      machine holding a given receipt key is Beam-attested.
  struct JobResult {
    bytes32 inputRoomKey;
    bytes32 outputRoomKey;
    bytes32 inputManifestSha256;
    bytes32 outputManifestSha256;
    string computeMember;
    Status status;
    uint8 level;
  }

  /// @notice An unanswered request. Only the fields of its kind are set; `transferKind` only for `Transfer`.
  struct OpenRequest {
    bytes32 key;
    RequestKind kind;
    TransferKind transferKind;
    address requester;
    uint64 openedAt;
    bytes32 requestId;
    bytes16 transferId;
    string room;
    string channel;
    string publication;
  }

  /// @notice Standard receipt entry of a report.
  struct StandardReceiptReport {
    bytes16 transferId;
    bytes32 requestId;
    uint8 status;
    uint64 totalBytes;
    uint64 bytesTransferred;
    uint64 completedAtMs;
    bytes32 planFingerprint;
    bytes32 coordinateChecksum;
    uint8 level;
  }

  /// @notice Request closure entry of a report: Beam's answer settles the request without a receipt.
  struct RequestClosure {
    bytes32 key;
    uint8 reason;
    bytes16 transferId;
  }

  /// @notice Room receipt entry of a report. `requestId` is the Room delivery it answers, or zero.
  struct RoomReceiptReport {
    string room;
    string channel;
    string publication;
    bytes32 requestId;
    uint8 status;
    uint64 fileSizeBytes;
    bytes32 manifestSha256;
    uint32 targetCount;
    bytes32 evidenceSha256;
    bytes32 assignmentsSha256;
    uint16 evidenceVersion;
    uint8 level;
  }

  /// @notice Job result entry of a report. Both Room receipts must be recorded, by this report or an earlier one.
  struct JobResultReport {
    bytes32 requestId;
    bytes32 inputRoomKey;
    bytes32 outputRoomKey;
    string computeMember;
  }

  error NotRequester(address caller);
  error NotRequestOwner(address caller);
  error InvalidRequestId();
  error InvalidTransferKind(uint8 kind);
  error InvalidTransferId();
  error InvalidRoomId();
  error InputTooLarge(uint256 length, uint256 max);
  error OpenRequestsFull(uint256 max);
  error UnknownRequest(bytes32 key);
  error InvalidStaleAfter();
  error InvalidRequestKey();
  error InvalidClosureReason(uint8 reason);
  error UnsupportedReportVersion(uint8 version);
  error InvalidStatus(uint8 status);
  error InvalidLevel(uint8 level);
  error InvalidEvidence();
  error InvalidJob();
  error MissingRoomReceipt(bytes32 roomKey);

  event TransferRequested(bytes32 indexed requestId, address indexed requester, TransferKind kind, string input);
  event ReceiptRequested(bytes16 indexed transferId, address indexed requester);
  event RoomReceiptRequested(
    bytes32 indexed roomKey, address indexed requester, string room, string channel, string publication
  );
  event RequestCancelled(bytes32 indexed key, address indexed by);
  /// @notice A Room receipt request was settled without a receipt. For `CLOSURE_BUCKET_ONLY_PUBLICATION`, follow up
  ///         with `requestReceipt(transferId)`.
  event RequestClosed(bytes32 indexed key, uint8 reason, bytes16 transferId);
  event ReceiptRecorded(
    bytes16 indexed transferId,
    bytes32 indexed requestId,
    Status status,
    uint64 totalBytes,
    uint64 bytesTransferred,
    uint8 level
  );
  /// @notice A standard `requestTransfer` id is now bound to the transfer that answers it.
  event RequestAnswered(bytes32 indexed requestId, bytes16 indexed transferId);
  /// @notice A Room delivery `requestTransfer` id is now bound to the Room receipt that answers it.
  event RoomRequestAnswered(bytes32 indexed requestId, bytes32 indexed roomKey);
  /// @notice A job `requestTransfer` id is answered by the result linking its input and output Room receipts.
  event JobRecorded(
    bytes32 indexed requestId, bytes32 inputRoomKey, bytes32 outputRoomKey, string computeMember, Status status, uint8 level
  );
  event RoomReceiptRecorded(
    bytes32 indexed roomKey,
    bytes32 indexed requestId,
    string room,
    string channel,
    string publication,
    Status status,
    uint64 fileSizeBytes,
    bytes32 evidenceSha256,
    uint8 level
  );
  event RequesterUpdated(address indexed requester, bool allowed);
  event StaleAfterUpdated(uint64 previousSeconds, uint64 newSeconds);

  mapping(bytes16 transferId => StandardReceipt) private s_receipts;
  mapping(bytes32 roomKey => RoomReceipt) private s_roomReceipts;
  mapping(bytes32 requestId => bytes16 transferId) private s_transferIdForRequest;
  mapping(bytes32 requestId => bytes32 roomKey) private s_roomKeyForRequest;
  mapping(bytes32 requestId => JobResult) private s_jobs;
  mapping(address account => bool) private s_requesters;

  bytes32[] private s_openKeys;
  mapping(bytes32 key => uint256 position) private s_openPosition;
  mapping(bytes32 key => OpenRequest) private s_openRequests;

  uint64 private s_staleAfter;

  /// @dev Whether reports are DON-signed CRE reports, which is what lets receipts carry `LEVEL_DON_ATTESTED`.
  bool private immutable i_donAttested;

  /// @param forwarderAddress The account allowed to deliver reports: the Chainlink forwarder of a CRE deployment
  ///        (the mock forwarder in simulation), or the relay's account of a relay deployment.
  /// @param staleAfterSeconds Age after which an open request is listed by `staleRequests()`.
  /// @param donAttestedReports True when a CRE workflow writes the receipts, false when a relay does.
  constructor(address forwarderAddress, uint64 staleAfterSeconds, bool donAttestedReports)
    ReceiverTemplate(forwarderAddress)
  {
    i_donAttested = donAttestedReports;
    _setStaleAfter(staleAfterSeconds);
  }

  modifier onlyRequester() {
    if (msg.sender != owner() && !s_requesters[msg.sender]) {
      revert NotRequester(msg.sender);
    }
    _;
  }

  // ---------------------------------------------------------------- requests

  /// @notice Asks for a Beam transfer, Room delivery or job identified by `requestId`.
  /// @dev A request that is already open or already answered is a no-op.
  /// @param requestId Caller-chosen id, unique per request. Offchain it is the lowercase 0x-prefixed hex string;
  ///        a standard transfer's id derives from it.
  /// @param kind `TransferKind.Standard` (1), `TransferKind.RoomDelivery` (2) or `TransferKind.Job` (3).
  /// @param input Opaque data for the workflow that starts the transfer.
  function requestTransfer(bytes32 requestId, uint8 kind, string calldata input) external onlyRequester {
    if (requestId == bytes32(0)) revert InvalidRequestId();
    if (kind == uint8(TransferKind.None) || kind > uint8(TransferKind.Job)) revert InvalidTransferKind(kind);
    if (bytes(input).length > MAX_INPUT_BYTES) revert InputTooLarge(bytes(input).length, MAX_INPUT_BYTES);
    bytes32 key = transferRequestKey(requestId);
    if (s_openPosition[key] != 0 || _isAnswered(requestId)) return;

    OpenRequest storage request = _open(key, RequestKind.Transfer);
    request.transferKind = TransferKind(kind);
    request.requestId = requestId;
    emit TransferRequested(requestId, msg.sender, TransferKind(kind), input);
  }

  /// @notice Asks for the receipt of an existing standard transfer.
  /// @dev A request that is already open, or whose receipt is already recorded, is a no-op.
  function requestReceipt(bytes16 transferId) external onlyRequester {
    if (transferId == bytes16(0)) revert InvalidTransferId();
    bytes32 key = receiptRequestKey(transferId);
    if (s_openPosition[key] != 0 || s_receipts[transferId].status != Status.None) return;

    OpenRequest storage request = _open(key, RequestKind.Receipt);
    request.transferId = transferId;
    emit ReceiptRequested(transferId, msg.sender);
  }

  /// @notice Asks for the receipt of one Room publication.
  /// @dev A request that is already open, or whose receipt is already recorded, is a no-op.
  function requestRoomReceipt(string calldata room, string calldata channel, string calldata publication)
    external
    onlyRequester
  {
    _checkRoomId(room);
    _checkRoomId(channel);
    _checkRoomId(publication);
    bytes32 roomKey = roomReceiptKey(room, channel, publication);
    bytes32 key = roomRequestKey(roomKey);
    if (s_openPosition[key] != 0 || s_roomReceipts[roomKey].status != Status.None) return;

    OpenRequest storage request = _open(key, RequestKind.RoomReceipt);
    request.room = room;
    request.channel = channel;
    request.publication = publication;
    emit RoomReceiptRequested(roomKey, msg.sender, room, channel, publication);
  }

  /// @notice Removes an open request that will never be answered. Callable by its requester or the owner.
  function cancelRequest(bytes32 key) external {
    if (s_openPosition[key] == 0) revert UnknownRequest(key);
    if (msg.sender != owner() && msg.sender != s_openRequests[key].requester) revert NotRequestOwner(msg.sender);
    _close(key);
    emit RequestCancelled(key, msg.sender);
  }

  // ---------------------------------------------------------------- admin

  /// @notice Allows or disallows `account` to open requests. The owner is always allowed.
  function setRequester(address account, bool allowed) external onlyOwner {
    s_requesters[account] = allowed;
    emit RequesterUpdated(account, allowed);
  }

  function setStaleAfter(uint64 staleAfterSeconds) external onlyOwner {
    _setStaleAfter(staleAfterSeconds);
  }

  // ---------------------------------------------------------------- views

  function getReceipt(bytes16 transferId) external view returns (StandardReceipt memory) {
    return s_receipts[transferId];
  }

  function getRoomReceipt(bytes32 roomKey) external view returns (RoomReceipt memory) {
    return s_roomReceipts[roomKey];
  }

  /// @notice The standard transfer that answered `requestId`, or zero.
  function transferIdForRequest(bytes32 requestId) external view returns (bytes16) {
    return s_transferIdForRequest[requestId];
  }

  /// @notice The Room receipt that answered `requestId`, or zero.
  function roomKeyForRequest(bytes32 requestId) external view returns (bytes32) {
    return s_roomKeyForRequest[requestId];
  }

  /// @notice The result that answered job `requestId`; `status` is `None` until there is one.
  function getJob(bytes32 requestId) external view returns (JobResult memory) {
    return s_jobs[requestId];
  }

  function isRequester(address account) external view returns (bool) {
    return account == owner() || s_requesters[account];
  }

  function staleAfter() external view returns (uint64) {
    return s_staleAfter;
  }

  /// @notice True when receipts are written by a CRE workflow as DON-signed reports, false for a relay.
  function donAttested() external view returns (bool) {
    return i_donAttested;
  }

  /// @notice The level every standard receipt of this deployment carries.
  function standardReceiptLevel() public view returns (uint8) {
    return LEVEL_BEAM_REPORTED | _attestationLevel();
  }

  /// @notice The level of a Room receipt without a signed receipt from every target.
  function roomReceiptLevel() public view returns (uint8) {
    return LEVEL_BEAM_REPORTED | _attestationLevel();
  }

  /// @notice The level of a Room receipt whose evidence holds a signed receipt from every target.
  function roomSignedReceiptLevel() public view returns (uint8) {
    return LEVEL_BEAM_REPORTED | LEVEL_TARGET_SIGNED | _attestationLevel();
  }

  function isOpen(bytes32 key) external view returns (bool) {
    return s_openPosition[key] != 0;
  }

  function openRequestCount() external view returns (uint256) {
    return s_openKeys.length;
  }

  /// @notice A page of the open-request set. The order is not chronological; use `openedAt`.
  function openRequests(uint256 offset, uint256 limit) external view returns (OpenRequest[] memory page) {
    uint256 total = s_openKeys.length;
    if (offset >= total) return new OpenRequest[](0);
    uint256 end = limit > total - offset ? total : offset + limit;
    page = new OpenRequest[](end - offset);
    for (uint256 i = offset; i < end; ++i) {
      page[i - offset] = s_openRequests[s_openKeys[i]];
    }
  }

  /// @notice Open requests older than `staleAfter()`. A non-empty result means the workflow is not keeping up,
  ///         or a request can never be answered and should be cancelled.
  function staleRequests() external view returns (OpenRequest[] memory stale) {
    uint256 total = s_openKeys.length;
    uint256 count;
    for (uint256 i = 0; i < total; ++i) {
      if (_isStale(s_openRequests[s_openKeys[i]])) ++count;
    }
    stale = new OpenRequest[](count);
    uint256 next;
    for (uint256 i = 0; i < total && next < count; ++i) {
      OpenRequest storage request = s_openRequests[s_openKeys[i]];
      if (_isStale(request)) stale[next++] = request;
    }
  }

  function roomReceiptKey(string memory room, string memory channel, string memory publication)
    public
    pure
    returns (bytes32)
  {
    return keccak256(abi.encode(room, channel, publication));
  }

  function transferRequestKey(bytes32 requestId) public pure returns (bytes32) {
    return keccak256(abi.encode(RequestKind.Transfer, requestId));
  }

  function receiptRequestKey(bytes16 transferId) public pure returns (bytes32) {
    return keccak256(abi.encode(RequestKind.Receipt, transferId));
  }

  function roomRequestKey(bytes32 roomKey) public pure returns (bytes32) {
    return keccak256(abi.encode(RequestKind.RoomReceipt, roomKey));
  }

  // ---------------------------------------------------------------- reports

  /// @dev Decodes `abi.encode(uint8 version, StandardReceiptReport[] standard, RoomReceiptReport[] rooms,
  ///      RequestClosure[] closures, JobResultReport[] jobs)`. Jobs are recorded last, so a report can carry a job
  ///      with its two Room receipts. A receipt or job that is already recorded and a closure for a request that is
  ///      not open are skipped, so a repeated report is a no-op.
  function _processReport(bytes calldata report) internal override {
    uint8 version = abi.decode(report, (uint8));
    if (version != REPORT_VERSION) revert UnsupportedReportVersion(version);
    (
      ,
      StandardReceiptReport[] memory standard,
      RoomReceiptReport[] memory rooms,
      RequestClosure[] memory closures,
      JobResultReport[] memory jobs
    ) = abi.decode(report, (uint8, StandardReceiptReport[], RoomReceiptReport[], RequestClosure[], JobResultReport[]));
    for (uint256 i = 0; i < standard.length; ++i) {
      _recordReceipt(standard[i]);
    }
    for (uint256 i = 0; i < rooms.length; ++i) {
      _recordRoomReceipt(rooms[i]);
    }
    for (uint256 i = 0; i < closures.length; ++i) {
      _closeRequest(closures[i]);
    }
    for (uint256 i = 0; i < jobs.length; ++i) {
      _recordJob(jobs[i]);
    }
  }

  function _recordReceipt(StandardReceiptReport memory entry) private {
    if (entry.transferId == bytes16(0)) revert InvalidTransferId();
    if (entry.status == uint8(Status.None) || entry.status > uint8(Status.Cancelled)) revert InvalidStatus(entry.status);
    if (entry.level != standardReceiptLevel()) revert InvalidLevel(entry.level);

    bool answers = _answerRequest(entry.requestId, TransferKind.Standard);
    if (answers) {
      s_transferIdForRequest[entry.requestId] = entry.transferId;
      emit RequestAnswered(entry.requestId, entry.transferId);
    }

    StandardReceipt storage receipt = s_receipts[entry.transferId];
    if (receipt.status != Status.None) {
      if (answers && receipt.requestId == bytes32(0)) receipt.requestId = entry.requestId;
      return;
    }
    bytes32 requestId = answers ? entry.requestId : bytes32(0);
    receipt.requestId = requestId;
    receipt.planFingerprint = entry.planFingerprint;
    receipt.coordinateChecksum = entry.coordinateChecksum;
    receipt.totalBytes = entry.totalBytes;
    receipt.bytesTransferred = entry.bytesTransferred;
    receipt.completedAtMs = entry.completedAtMs;
    receipt.status = Status(entry.status);
    receipt.level = entry.level;
    _close(receiptRequestKey(entry.transferId));
    emit ReceiptRecorded(
      entry.transferId, requestId, Status(entry.status), entry.totalBytes, entry.bytesTransferred, entry.level
    );
  }

  function _recordRoomReceipt(RoomReceiptReport memory entry) private {
    if (entry.status == uint8(Status.None) || entry.status > uint8(Status.Partial)) revert InvalidStatus(entry.status);
    if (
      entry.level != roomReceiptLevel()
        && (entry.level != roomSignedReceiptLevel() || entry.targetCount == 0)
    ) revert InvalidLevel(entry.level);
    if (entry.evidenceVersion == 0 || entry.evidenceSha256 == bytes32(0)) revert InvalidEvidence();

    bytes32 roomKey = roomReceiptKey(entry.room, entry.channel, entry.publication);
    bool answers = _answerRequest(entry.requestId, TransferKind.RoomDelivery);
    if (answers) {
      s_roomKeyForRequest[entry.requestId] = roomKey;
      emit RoomRequestAnswered(entry.requestId, roomKey);
    }

    RoomReceipt storage receipt = s_roomReceipts[roomKey];
    if (receipt.status != Status.None) return;

    receipt.manifestSha256 = entry.manifestSha256;
    receipt.evidenceSha256 = entry.evidenceSha256;
    receipt.assignmentsSha256 = entry.assignmentsSha256;
    receipt.fileSizeBytes = entry.fileSizeBytes;
    receipt.targetCount = entry.targetCount;
    receipt.evidenceVersion = entry.evidenceVersion;
    receipt.status = Status(entry.status);
    receipt.level = entry.level;
    _close(roomRequestKey(roomKey));
    emit RoomReceiptRecorded(
      roomKey,
      answers ? entry.requestId : bytes32(0),
      entry.room,
      entry.channel,
      entry.publication,
      Status(entry.status),
      entry.fileSizeBytes,
      entry.evidenceSha256,
      entry.level
    );
  }

  /// @dev Answers a job request with the result linking its two recorded Room receipts.
  function _recordJob(JobResultReport memory entry) private {
    uint256 memberLength = bytes(entry.computeMember).length;
    if (
      entry.requestId == bytes32(0) || entry.inputRoomKey == entry.outputRoomKey || memberLength == 0
        || memberLength > MAX_ROOM_ID_BYTES
    ) revert InvalidJob();
    RoomReceipt storage input = s_roomReceipts[entry.inputRoomKey];
    RoomReceipt storage output = s_roomReceipts[entry.outputRoomKey];
    if (input.status == Status.None) revert MissingRoomReceipt(entry.inputRoomKey);
    if (output.status == Status.None) revert MissingRoomReceipt(entry.outputRoomKey);
    if (!_answerRequest(entry.requestId, TransferKind.Job)) return;

    JobResult storage job = s_jobs[entry.requestId];
    job.inputRoomKey = entry.inputRoomKey;
    job.outputRoomKey = entry.outputRoomKey;
    job.inputManifestSha256 = input.manifestSha256;
    job.outputManifestSha256 = output.manifestSha256;
    job.computeMember = entry.computeMember;
    job.status = input.status == Status.Completed ? output.status : input.status;
    job.level = input.level & output.level;
    emit JobRecorded(entry.requestId, entry.inputRoomKey, entry.outputRoomKey, entry.computeMember, job.status, job.level);
  }

  /// @dev Closes an open Room receipt request that Beam settled without a receipt. Requests of other kinds, and
  ///      keys that are not open, are left alone.
  function _closeRequest(RequestClosure memory closure) private {
    if (closure.key == bytes32(0)) revert InvalidRequestKey();
    if (closure.reason != CLOSURE_BUCKET_ONLY_PUBLICATION) revert InvalidClosureReason(closure.reason);
    if (s_openPosition[closure.key] == 0 || s_openRequests[closure.key].kind != RequestKind.RoomReceipt) return;
    _close(closure.key);
    emit RequestClosed(closure.key, closure.reason, closure.transferId);
  }

  /// @dev Binds an unanswered `requestId` and closes its open request, unless that request asked for the other
  ///      kind. A request id that was never opened is bound too, so a push that lands first still answers it.
  function _answerRequest(bytes32 requestId, TransferKind kind) private returns (bool) {
    if (requestId == bytes32(0) || _isAnswered(requestId)) return false;
    bytes32 key = transferRequestKey(requestId);
    if (s_openPosition[key] != 0 && s_openRequests[key].transferKind != kind) return false;
    _close(key);
    return true;
  }

  function _attestationLevel() private view returns (uint8) {
    return i_donAttested ? LEVEL_DON_ATTESTED : 0;
  }

  function _isAnswered(bytes32 requestId) private view returns (bool) {
    return s_transferIdForRequest[requestId] != bytes16(0) || s_roomKeyForRequest[requestId] != bytes32(0)
      || s_jobs[requestId].status != Status.None;
  }

  // ---------------------------------------------------------------- open-request set

  function _open(bytes32 key, RequestKind kind) private returns (OpenRequest storage request) {
    if (s_openKeys.length >= MAX_OPEN_REQUESTS) revert OpenRequestsFull(MAX_OPEN_REQUESTS);
    s_openKeys.push(key);
    s_openPosition[key] = s_openKeys.length;
    request = s_openRequests[key];
    request.key = key;
    request.kind = kind;
    request.requester = msg.sender;
    request.openedAt = uint64(block.timestamp);
  }

  function _close(bytes32 key) private {
    uint256 position = s_openPosition[key];
    if (position == 0) return;
    uint256 last = s_openKeys.length;
    if (position != last) {
      bytes32 moved = s_openKeys[last - 1];
      s_openKeys[position - 1] = moved;
      s_openPosition[moved] = position;
    }
    s_openKeys.pop();
    delete s_openPosition[key];
    delete s_openRequests[key];
  }

  function _isStale(OpenRequest storage request) private view returns (bool) {
    return block.timestamp - request.openedAt >= s_staleAfter;
  }

  function _checkRoomId(string calldata id) private pure {
    uint256 length = bytes(id).length;
    if (length == 0 || length > MAX_ROOM_ID_BYTES) revert InvalidRoomId();
  }

  function _setStaleAfter(uint64 staleAfterSeconds) private {
    if (staleAfterSeconds == 0) revert InvalidStaleAfter();
    emit StaleAfterUpdated(s_staleAfter, staleAfterSeconds);
    s_staleAfter = staleAfterSeconds;
  }
}
