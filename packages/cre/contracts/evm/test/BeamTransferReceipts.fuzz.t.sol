// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {BeamTransferReceipts} from "../src/BeamTransferReceipts.sol";
import {ReceiverTemplate} from "../src/vendor/chainlink/ReceiverTemplate.sol";

/// Foundry cheatcodes used here, declared locally so the project needs no forge-std install.
interface FuzzVm {
  function prank(address msgSender) external;
  function warp(uint256 newTimestamp) external;
  function assume(bool condition) external;
  function expectRevert(bytes calldata revertData) external;
}

/// Report builders shared by the fuzz and invariant suites. Reports carry no workflow identity check: these
/// deployments set no expected author, id or name, so the forwarder check is the only gate.
abstract contract ReceiptsKit {
  FuzzVm internal constant vm = FuzzVm(address(uint160(uint256(keccak256("hevm cheat code")))));

  address internal constant FORWARDER = address(0xF0);
  address internal constant RELAY = address(0xD4);
  address internal constant REQUESTER = address(0xB2);
  uint64 internal constant STALE_AFTER = 1 days;
  string internal constant ROOM = "room";
  string internal constant CHANNEL = "channel";
  string internal constant COMPUTE = "btr_member_cccccccccccccccccccccccccc";

  function _standardEntry(BeamTransferReceipts target, bytes16 transferId, bytes32 requestId, uint64 seed)
    internal
    view
    returns (BeamTransferReceipts.StandardReceiptReport memory)
  {
    return BeamTransferReceipts.StandardReceiptReport({
      transferId: transferId,
      requestId: requestId,
      status: uint8(BeamTransferReceipts.Status.Completed),
      totalBytes: seed,
      bytesTransferred: seed / 2,
      completedAtMs: seed ^ 1,
      planFingerprint: keccak256(abi.encode("plan", seed)),
      coordinateChecksum: keccak256(abi.encode("coordinates", seed)),
      level: target.standardReceiptLevel()
    });
  }

  function _roomEntry(BeamTransferReceipts target, string memory publication, bytes32 requestId, uint64 seed)
    internal
    view
    returns (BeamTransferReceipts.RoomReceiptReport memory)
  {
    return BeamTransferReceipts.RoomReceiptReport({
      room: ROOM,
      channel: CHANNEL,
      publication: publication,
      requestId: requestId,
      status: uint8(BeamTransferReceipts.Status.Completed),
      fileSizeBytes: seed,
      manifestSha256: keccak256(abi.encode("manifest", seed)),
      targetCount: 2,
      evidenceSha256: keccak256(abi.encode("evidence", seed)),
      assignmentsSha256: keccak256(abi.encode("assignments", seed)),
      evidenceVersion: 1,
      level: target.roomSignedReceiptLevel()
    });
  }

  function _standardReport(BeamTransferReceipts.StandardReceiptReport memory entry) internal pure returns (bytes memory) {
    BeamTransferReceipts.StandardReceiptReport[] memory standard = new BeamTransferReceipts.StandardReceiptReport[](1);
    standard[0] = entry;
    return abi.encode(
      uint8(2),
      standard,
      new BeamTransferReceipts.RoomReceiptReport[](0),
      new BeamTransferReceipts.RequestClosure[](0),
      new BeamTransferReceipts.JobResultReport[](0)
    );
  }

  function _roomReport(BeamTransferReceipts.RoomReceiptReport memory entry) internal pure returns (bytes memory) {
    BeamTransferReceipts.RoomReceiptReport[] memory rooms = new BeamTransferReceipts.RoomReceiptReport[](1);
    rooms[0] = entry;
    return abi.encode(
      uint8(2),
      new BeamTransferReceipts.StandardReceiptReport[](0),
      rooms,
      new BeamTransferReceipts.RequestClosure[](0),
      new BeamTransferReceipts.JobResultReport[](0)
    );
  }

  function _closureReport(bytes32 key, bytes16 transferId) internal pure returns (bytes memory) {
    BeamTransferReceipts.RequestClosure[] memory closures = new BeamTransferReceipts.RequestClosure[](1);
    closures[0] = BeamTransferReceipts.RequestClosure({key: key, reason: 1, transferId: transferId});
    return abi.encode(
      uint8(2),
      new BeamTransferReceipts.StandardReceiptReport[](0),
      new BeamTransferReceipts.RoomReceiptReport[](0),
      closures,
      new BeamTransferReceipts.JobResultReport[](0)
    );
  }

  function _jobReport(bytes32 requestId, bytes32 inputRoomKey, bytes32 outputRoomKey) internal pure returns (bytes memory) {
    BeamTransferReceipts.JobResultReport[] memory jobs = new BeamTransferReceipts.JobResultReport[](1);
    jobs[0] = BeamTransferReceipts.JobResultReport({
      requestId: requestId,
      inputRoomKey: inputRoomKey,
      outputRoomKey: outputRoomKey,
      computeMember: COMPUTE
    });
    return abi.encode(
      uint8(2),
      new BeamTransferReceipts.StandardReceiptReport[](0),
      new BeamTransferReceipts.RoomReceiptReport[](0),
      new BeamTransferReceipts.RequestClosure[](0),
      jobs
    );
  }

  function _metadata() internal pure returns (bytes memory) {
    return new bytes(64);
  }

  function _text(uint256 length) internal pure returns (string memory) {
    bytes memory text = new bytes(length);
    for (uint256 i = 0; i < length; ++i) {
      text[i] = "a";
    }
    return string(text);
  }

  function _check(bool condition, string memory message) internal pure {
    if (!condition) revert(message);
  }
}

/// Each input bound and access rule, over random inputs, on a CRE deployment and a relay deployment.
contract BeamTransferReceiptsFuzzTest is ReceiptsKit {
  BeamTransferReceipts private cre;
  BeamTransferReceipts private relay;

  function setUp() public {
    vm.warp(1_760_000_000);
    cre = new BeamTransferReceipts(FORWARDER, STALE_AFTER, true);
    relay = new BeamTransferReceipts(RELAY, STALE_AFTER, false);
    cre.setRequester(REQUESTER, true);
    relay.setRequester(REQUESTER, true);
  }

  function testFuzz_onlyTheForwarderDeliversReports(address caller) public {
    vm.assume(caller != FORWARDER);
    bytes memory report = _standardReport(_standardEntry(cre, bytes16(uint128(1)), bytes32(0), 1));
    bytes memory metadata = _metadata();

    vm.expectRevert(abi.encodeWithSelector(ReceiverTemplate.InvalidSender.selector, caller, FORWARDER));
    vm.prank(caller);
    cre.onReport(metadata, report);
  }

  function testFuzz_onlyTheOwnerAndRequestersOpenRequests(address caller, bytes16 transferId) public {
    vm.assume(caller != address(this) && caller != REQUESTER && transferId != bytes16(0));

    vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.NotRequester.selector, caller));
    vm.prank(caller);
    cre.requestReceipt(transferId);

    vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.NotRequester.selector, caller));
    vm.prank(caller);
    cre.requestTransfer(bytes32(uint256(1)), 1, "");
  }

  function testFuzz_onlyTheRequesterAndTheOwnerCancel(address caller, bytes16 transferId) public {
    vm.assume(caller != address(this) && caller != REQUESTER && transferId != bytes16(0));
    vm.prank(REQUESTER);
    cre.requestReceipt(transferId);
    bytes32 key = cre.receiptRequestKey(transferId);

    vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.NotRequestOwner.selector, caller));
    vm.prank(caller);
    cre.cancelRequest(key);

    vm.prank(REQUESTER);
    cre.cancelRequest(key);
    _check(!cre.isOpen(key), "the requester cancels its request");
  }

  function testFuzz_transferKindsAreOneToThree(uint8 kind, bytes32 requestId) public {
    vm.assume(requestId != bytes32(0));
    bool valid = kind >= 1 && kind <= 3;
    if (!valid) vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.InvalidTransferKind.selector, kind));
    vm.prank(REQUESTER);
    cre.requestTransfer(requestId, kind, "");
    if (valid) _check(cre.isOpen(cre.transferRequestKey(requestId)), "a valid kind opens a request");
  }

  /// Half the runs land within two bytes of the bound, where an off-by-one would show.
  function testFuzz_transferInputIsBounded(uint16 seed) public {
    uint256 max = cre.MAX_INPUT_BYTES();
    uint256 length = seed % 2 == 0 ? max - 2 + (seed / 2) % 5 : uint256(seed) % (2 * max + 1);
    string memory input = _text(length);
    bytes32 requestId = bytes32(uint256(1));
    bool valid = length <= cre.MAX_INPUT_BYTES();
    if (!valid) {
      vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.InputTooLarge.selector, length, cre.MAX_INPUT_BYTES()));
    }
    vm.prank(REQUESTER);
    cre.requestTransfer(requestId, 1, input);
    if (valid) _check(cre.isOpen(cre.transferRequestKey(requestId)), "an input within the bound opens a request");
  }

  function testFuzz_roomIdsAreBounded(uint8 roomLength, uint8 channelLength, uint8 publicationLength) public {
    uint256 max = cre.MAX_ROOM_ID_BYTES();
    bool valid = roomLength >= 1 && roomLength <= max && channelLength >= 1 && channelLength <= max
      && publicationLength >= 1 && publicationLength <= max;
    string memory room = _text(roomLength);
    string memory channel = _text(channelLength);
    string memory publication = _text(publicationLength);
    if (!valid) vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.InvalidRoomId.selector));
    vm.prank(REQUESTER);
    cre.requestRoomReceipt(room, channel, publication);
    if (valid) _check(cre.openRequestCount() == 1, "valid ids open a Room receipt request");
  }

  function testFuzz_standardReceiptsNeedAKnownStatusAndTheDeploymentsLevel(uint8 status, uint8 level, bool onRelay)
    public
  {
    BeamTransferReceipts target = onRelay ? relay : cre;
    bytes16 transferId = bytes16(uint128(1));
    BeamTransferReceipts.StandardReceiptReport memory entry = _standardEntry(target, transferId, bytes32(0), 1);
    entry.status = status;
    entry.level = level;
    bytes memory report = _standardReport(entry);
    bytes memory metadata = _metadata();

    if (status == 0 || status > uint8(BeamTransferReceipts.Status.Cancelled)) {
      vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.InvalidStatus.selector, status));
    } else if (level != target.standardReceiptLevel()) {
      vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.InvalidLevel.selector, level));
    }
    vm.prank(onRelay ? RELAY : FORWARDER);
    target.onReport(metadata, report);

    BeamTransferReceipts.StandardReceipt memory receipt = target.getReceipt(transferId);
    if (receipt.status != BeamTransferReceipts.Status.None) {
      _check(receipt.level == target.standardReceiptLevel(), "a recorded level is the deployment's");
      _check((receipt.level & target.LEVEL_DON_ATTESTED() != 0) == !onRelay, "only a CRE deployment attests");
    }
  }

  /// Two thirds of the runs use one of the deployment's two Room levels, so each meets a zero target count often.
  function testFuzz_roomReceiptsNeedAKnownStatusAndAnAllowedLevel(
    uint8 status,
    uint8 levelChoice,
    uint8 anyLevel,
    uint32 targetCount,
    bool onRelay
  ) public {
    BeamTransferReceipts target = onRelay ? relay : cre;
    uint8 level = levelChoice % 3 == 0
      ? target.roomReceiptLevel()
      : levelChoice % 3 == 1 ? target.roomSignedReceiptLevel() : anyLevel;
    if (levelChoice % 2 == 0) targetCount = targetCount % 2;
    BeamTransferReceipts.RoomReceiptReport memory entry = _roomEntry(target, "pub", bytes32(0), 1);
    entry.status = status;
    entry.level = level;
    entry.targetCount = targetCount;
    bytes memory report = _roomReport(entry);
    bytes memory metadata = _metadata();
    bool levelAllowed =
      level == target.roomReceiptLevel() || (level == target.roomSignedReceiptLevel() && targetCount > 0);

    if (status == 0 || status > uint8(BeamTransferReceipts.Status.Partial)) {
      vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.InvalidStatus.selector, status));
    } else if (!levelAllowed) {
      vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.InvalidLevel.selector, level));
    }
    vm.prank(onRelay ? RELAY : FORWARDER);
    target.onReport(metadata, report);

    BeamTransferReceipts.RoomReceipt memory receipt = target.getRoomReceipt(target.roomReceiptKey(ROOM, CHANNEL, "pub"));
    if (receipt.status != BeamTransferReceipts.Status.None) {
      _check(receipt.level == level, "the reported level is recorded");
      _check((receipt.level & target.LEVEL_DON_ATTESTED() != 0) == !onRelay, "only a CRE deployment attests");
    }
  }

  function testFuzz_aRecordedReceiptIsNeverOverwritten(bytes16 transferId, uint64 first, uint64 second) public {
    vm.assume(transferId != bytes16(0) && first != second);
    bytes memory firstReport = _standardReport(_standardEntry(cre, transferId, bytes32(0), first));
    BeamTransferReceipts.StandardReceiptReport memory later = _standardEntry(cre, transferId, bytes32(0), second);
    later.status = uint8(BeamTransferReceipts.Status.Failed);
    bytes memory laterReport = _standardReport(later);

    vm.prank(FORWARDER);
    cre.onReport(_metadata(), firstReport);
    BeamTransferReceipts.StandardReceipt memory before = cre.getReceipt(transferId);
    vm.prank(FORWARDER);
    cre.onReport(_metadata(), laterReport);

    BeamTransferReceipts.StandardReceipt memory current = cre.getReceipt(transferId);
    _check(keccak256(abi.encode(current)) == keccak256(abi.encode(before)), "a recorded receipt keeps its first values");
  }
}

/// Drives a deployment with requests, cancels, reports and time over small id pools, so ids collide often, and
/// keeps the first recorded value of each receipt, Room receipt, job and request binding.
contract ReceiptsHandler is ReceiptsKit {
  BeamTransferReceipts public immutable receipts;
  address public immutable forwarder;

  uint256 internal constant TRANSFERS = 8;
  uint256 internal constant REQUESTS = 8;
  uint256 internal constant PUBLICATIONS = 6;

  mapping(bytes16 => bytes) internal s_firstReceipt;
  mapping(bytes32 => bytes) internal s_firstRoomReceipt;
  mapping(bytes32 => bytes) internal s_firstJob;
  mapping(bytes32 => bytes) internal s_firstBinding;

  uint256 public recorded;
  uint256 public cancelled;
  uint256 public jobs;

  constructor(BeamTransferReceipts target, address reportForwarder) {
    receipts = target;
    forwarder = reportForwarder;
  }

  // ---------------------------------------------------------------- actions

  function requestTransfer(uint256 requestSeed, uint8 kind, uint8 inputLength) external {
    receipts.requestTransfer(requestId(requestSeed), 1 + kind % 3, _text(inputLength % 64));
    _snapshot();
  }

  function requestReceipt(uint256 transferSeed) external {
    receipts.requestReceipt(transferId(transferSeed));
    _snapshot();
  }

  function requestRoomReceipt(uint256 publicationSeed) external {
    receipts.requestRoomReceipt(ROOM, CHANNEL, publication(publicationSeed));
    _snapshot();
  }

  function cancelRequest(uint256 index) external {
    uint256 count = receipts.openRequestCount();
    if (count == 0) return;
    receipts.cancelRequest(receipts.openRequests(index % count, 1)[0].key);
    ++cancelled;
    _snapshot();
  }

  function deliverStandard(uint256 transferSeed, uint256 requestSeed, uint8 status, bool answers, uint64 seed)
    external
  {
    BeamTransferReceipts.StandardReceiptReport memory entry =
      _standardEntry(receipts, transferId(transferSeed), answers ? requestId(requestSeed) : bytes32(0), seed);
    entry.status = 1 + status % 3;
    _deliver(_standardReport(entry));
  }

  function deliverRoom(uint256 publicationSeed, uint256 requestSeed, uint8 status, bool signed, bool answers, uint64 seed)
    external
  {
    BeamTransferReceipts.RoomReceiptReport memory entry =
      _roomEntry(receipts, publication(publicationSeed), answers ? requestId(requestSeed) : bytes32(0), seed);
    entry.status = 1 + status % 4;
    if (!signed) {
      entry.level = receipts.roomReceiptLevel();
      entry.targetCount = uint32(seed % 3);
    }
    _deliver(_roomReport(entry));
  }

  function deliverClosure(uint256 index, bool ofAnOpenRequest) external {
    uint256 count = receipts.openRequestCount();
    bytes32 key = ofAnOpenRequest && count > 0
      ? receipts.openRequests(index % count, 1)[0].key
      : keccak256(abi.encode("unknown", index));
    _deliver(_closureReport(key, transferId(index)));
  }

  function deliverJob(uint256 requestSeed, uint256 inputSeed, uint256 outputSeed) external {
    if (inputSeed % PUBLICATIONS == outputSeed % PUBLICATIONS) return;
    _deliver(
      _jobReport(
        requestId(requestSeed),
        receipts.roomReceiptKey(ROOM, CHANNEL, publication(inputSeed)),
        receipts.roomReceiptKey(ROOM, CHANNEL, publication(outputSeed))
      )
    );
  }

  function passTime(uint32 seconds_) external {
    vm.warp(block.timestamp + seconds_ % 3 days);
  }

  // ---------------------------------------------------------------- pools and first values

  function transferId(uint256 seed) public pure returns (bytes16) {
    return bytes16(uint128(seed % TRANSFERS + 1));
  }

  function requestId(uint256 seed) public pure returns (bytes32) {
    return bytes32(seed % REQUESTS + 1);
  }

  function publication(uint256 seed) public pure returns (string memory) {
    return string(abi.encodePacked("pub-", bytes1(uint8(48 + seed % PUBLICATIONS))));
  }

  function transferCount() external pure returns (uint256) {
    return TRANSFERS;
  }

  function requestCount() external pure returns (uint256) {
    return REQUESTS;
  }

  function publicationCount() external pure returns (uint256) {
    return PUBLICATIONS;
  }

  function firstReceipt(bytes16 id) external view returns (bytes memory) {
    return s_firstReceipt[id];
  }

  function firstRoomReceipt(bytes32 roomKey) external view returns (bytes memory) {
    return s_firstRoomReceipt[roomKey];
  }

  function firstJob(bytes32 id) external view returns (bytes memory) {
    return s_firstJob[id];
  }

  function firstBinding(bytes32 id) external view returns (bytes memory) {
    return s_firstBinding[id];
  }

  function _deliver(bytes memory report) private {
    vm.prank(forwarder);
    receipts.onReport(_metadata(), report);
    _snapshot();
  }

  /// Keeps the first recorded value of everything in the pools. A receipt's `requestId` may be bound once,
  /// from zero; that is the only later change the contract allows, so it is the only one kept.
  function _snapshot() private {
    for (uint256 i = 0; i < TRANSFERS; ++i) {
      bytes16 id = transferId(i);
      BeamTransferReceipts.StandardReceipt memory receipt = receipts.getReceipt(id);
      if (receipt.status == BeamTransferReceipts.Status.None) continue;
      if (s_firstReceipt[id].length == 0) {
        s_firstReceipt[id] = abi.encode(receipt);
        ++recorded;
        continue;
      }
      BeamTransferReceipts.StandardReceipt memory first =
        abi.decode(s_firstReceipt[id], (BeamTransferReceipts.StandardReceipt));
      if (first.requestId == bytes32(0) && receipt.requestId != bytes32(0)) {
        first.requestId = receipt.requestId;
        s_firstReceipt[id] = abi.encode(first);
      }
    }
    for (uint256 i = 0; i < PUBLICATIONS; ++i) {
      bytes32 roomKey = receipts.roomReceiptKey(ROOM, CHANNEL, publication(i));
      BeamTransferReceipts.RoomReceipt memory receipt = receipts.getRoomReceipt(roomKey);
      if (receipt.status != BeamTransferReceipts.Status.None && s_firstRoomReceipt[roomKey].length == 0) {
        s_firstRoomReceipt[roomKey] = abi.encode(receipt);
        ++recorded;
      }
    }
    for (uint256 i = 0; i < REQUESTS; ++i) {
      bytes32 id = requestId(i);
      BeamTransferReceipts.JobResult memory job = receipts.getJob(id);
      if (job.status != BeamTransferReceipts.Status.None && s_firstJob[id].length == 0) {
        s_firstJob[id] = abi.encode(job);
        ++jobs;
      }
      bytes memory binding = abi.encode(receipts.transferIdForRequest(id), receipts.roomKeyForRequest(id));
      if (s_firstBinding[id].length == 0 && keccak256(binding) != keccak256(abi.encode(bytes16(0), bytes32(0)))) {
        s_firstBinding[id] = binding;
      }
    }
  }
}

/// The properties every deployment keeps, whatever sequence of calls reaches it.
abstract contract ReceiptsInvariants is ReceiptsKit {
  event log_named_uint(string key, uint256 val);

  BeamTransferReceipts internal receipts;
  ReceiptsHandler internal handler;

  function _deploy(address reportForwarder, bool donAttested) internal {
    vm.warp(1_760_000_000);
    receipts = new BeamTransferReceipts(reportForwarder, STALE_AFTER, donAttested);
    handler = new ReceiptsHandler(receipts, reportForwarder);
    receipts.setRequester(address(handler), true);
  }

  function targetContracts() public view returns (address[] memory targets) {
    targets = new address[](1);
    targets[0] = address(handler);
  }

  /// The open set is bounded, holds no key twice, and keys each request by its own fields.
  function invariant_openSetIsBoundedAndKeyedByItsFields() public view {
    uint256 count = receipts.openRequestCount();
    _check(count <= receipts.MAX_OPEN_REQUESTS(), "the open set is bounded");
    BeamTransferReceipts.OpenRequest[] memory open = receipts.openRequests(0, count);
    _check(open.length == count, "the page holds the whole set");
    for (uint256 i = 0; i < open.length; ++i) {
      _check(receipts.isOpen(open[i].key), "every listed request is open");
      for (uint256 j = i + 1; j < open.length; ++j) {
        _check(open[i].key != open[j].key, "no key is open twice");
      }
      _check(open[i].key == _keyOf(open[i]), "a request's key derives from its fields");
    }
  }

  /// A request is open only while nothing answers it.
  function invariant_answeredRequestsAreNeverOpen() public view {
    BeamTransferReceipts.OpenRequest[] memory open = receipts.openRequests(0, receipts.openRequestCount());
    for (uint256 i = 0; i < open.length; ++i) {
      BeamTransferReceipts.OpenRequest memory request = open[i];
      if (request.kind == BeamTransferReceipts.RequestKind.Receipt) {
        _check(
          receipts.getReceipt(request.transferId).status == BeamTransferReceipts.Status.None,
          "a recorded receipt has no open request"
        );
      } else if (request.kind == BeamTransferReceipts.RequestKind.RoomReceipt) {
        bytes32 roomKey = receipts.roomReceiptKey(request.room, request.channel, request.publication);
        _check(
          receipts.getRoomReceipt(roomKey).status == BeamTransferReceipts.Status.None,
          "a recorded Room receipt has no open request"
        );
      } else {
        _check(_answeredKinds(request.requestId) == 0, "an answered request id has no open request");
      }
    }
  }

  /// A recorded receipt, Room receipt or job never changes; a receipt's request id is bound at most once.
  function invariant_recordsAreFinal() public view {
    for (uint256 i = 0; i < handler.transferCount(); ++i) {
      bytes16 id = handler.transferId(i);
      bytes memory first = handler.firstReceipt(id);
      if (first.length == 0) continue;
      _check(keccak256(abi.encode(receipts.getReceipt(id))) == keccak256(first), "a receipt keeps its first values");
    }
    for (uint256 i = 0; i < handler.publicationCount(); ++i) {
      bytes32 roomKey = receipts.roomReceiptKey(ROOM, CHANNEL, handler.publication(i));
      bytes memory first = handler.firstRoomReceipt(roomKey);
      if (first.length == 0) continue;
      _check(
        keccak256(abi.encode(receipts.getRoomReceipt(roomKey))) == keccak256(first), "a Room receipt keeps its first values"
      );
    }
    for (uint256 i = 0; i < handler.requestCount(); ++i) {
      bytes32 id = handler.requestId(i);
      bytes memory job = handler.firstJob(id);
      if (job.length != 0) {
        _check(keccak256(abi.encode(receipts.getJob(id))) == keccak256(job), "a job keeps its first result");
      }
      bytes memory binding = handler.firstBinding(id);
      if (binding.length != 0) {
        _check(
          keccak256(abi.encode(receipts.transferIdForRequest(id), receipts.roomKeyForRequest(id))) == keccak256(binding),
          "a request id stays bound to its first answer"
        );
      }
    }
  }

  /// A request id answers one kind: a standard transfer, a Room delivery or a job.
  function invariant_eachRequestIdAnswersOneKind() public view {
    for (uint256 i = 0; i < handler.requestCount(); ++i) {
      _check(_answeredKinds(handler.requestId(i)) <= 1, "a request id answers at most one kind");
    }
  }

  /// Every recorded level is one the deployment allows, and only a CRE deployment sets the DON bit.
  function invariant_levelsMatchTheDeployment() public view {
    uint8 don = receipts.LEVEL_DON_ATTESTED();
    bool attested = receipts.donAttested();
    for (uint256 i = 0; i < handler.transferCount(); ++i) {
      BeamTransferReceipts.StandardReceipt memory receipt = receipts.getReceipt(handler.transferId(i));
      if (receipt.status == BeamTransferReceipts.Status.None) continue;
      _check(receipt.level == receipts.standardReceiptLevel(), "a receipt's level is the deployment's");
      _check((receipt.level & don != 0) == attested, "the DON bit follows the deployment");
    }
    for (uint256 i = 0; i < handler.publicationCount(); ++i) {
      BeamTransferReceipts.RoomReceipt memory receipt =
        receipts.getRoomReceipt(receipts.roomReceiptKey(ROOM, CHANNEL, handler.publication(i)));
      if (receipt.status == BeamTransferReceipts.Status.None) continue;
      _check(
        receipt.level == receipts.roomReceiptLevel()
          || (receipt.level == receipts.roomSignedReceiptLevel() && receipt.targetCount > 0),
        "a Room receipt's level is allowed"
      );
      _check((receipt.level & don != 0) == attested, "the DON bit follows the deployment");
    }
    for (uint256 i = 0; i < handler.requestCount(); ++i) {
      BeamTransferReceipts.JobResult memory job = receipts.getJob(handler.requestId(i));
      if (job.status == BeamTransferReceipts.Status.None) continue;
      uint8 both =
        receipts.getRoomReceipt(job.inputRoomKey).level & receipts.getRoomReceipt(job.outputRoomKey).level;
      _check(job.level == both, "a job holds the claims both Room receipts carry");
    }
  }

  /// Reports how much of the state space a run reached, with `-vv`.
  function afterInvariant() public {
    emit log_named_uint("receipts recorded", handler.recorded());
    emit log_named_uint("requests cancelled", handler.cancelled());
    emit log_named_uint("jobs recorded", handler.jobs());
    emit log_named_uint("requests open", receipts.openRequestCount());
  }

  function _keyOf(BeamTransferReceipts.OpenRequest memory request) private view returns (bytes32) {
    if (request.kind == BeamTransferReceipts.RequestKind.Transfer) return receipts.transferRequestKey(request.requestId);
    if (request.kind == BeamTransferReceipts.RequestKind.Receipt) return receipts.receiptRequestKey(request.transferId);
    return receipts.roomRequestKey(receipts.roomReceiptKey(request.room, request.channel, request.publication));
  }

  function _answeredKinds(bytes32 id) private view returns (uint256 kinds) {
    if (receipts.transferIdForRequest(id) != bytes16(0)) ++kinds;
    if (receipts.roomKeyForRequest(id) != bytes32(0)) ++kinds;
    if (receipts.getJob(id).status != BeamTransferReceipts.Status.None) ++kinds;
  }
}

contract BeamTransferReceiptsCreInvariantTest is ReceiptsInvariants {
  function setUp() public {
    _deploy(FORWARDER, true);
  }
}

contract BeamTransferReceiptsRelayInvariantTest is ReceiptsInvariants {
  function setUp() public {
    _deploy(RELAY, false);
  }
}
