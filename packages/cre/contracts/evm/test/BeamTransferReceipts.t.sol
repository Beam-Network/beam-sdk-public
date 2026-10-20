// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {BeamTransferReceipts} from "../src/BeamTransferReceipts.sol";
import {ReceiverTemplate} from "../src/vendor/chainlink/ReceiverTemplate.sol";
import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";

/// Foundry cheatcodes used here, declared locally so the project needs no forge-std install.
interface Vm {
  struct Log {
    bytes32[] topics;
    bytes data;
    address emitter;
  }

  function prank(address msgSender) external;
  function warp(uint256 newTimestamp) external;
  function expectRevert(bytes4 revertData) external;
  function expectRevert(bytes calldata revertData) external;
  function recordLogs() external;
  function getRecordedLogs() external returns (Log[] memory logs);
}

contract BeamTransferReceiptsTest {
  event log_named_uint(string key, uint256 val);

  Vm private constant vm = Vm(address(uint160(uint256(keccak256("hevm cheat code")))));

  address private constant FORWARDER = address(0xF0);
  address private constant AUTHOR = address(0xA1);
  address private constant REQUESTER = address(0xB2);
  address private constant STRANGER = address(0xC3);
  address private constant RELAY = address(0xD4);
  bytes32 private constant WORKFLOW_ID = bytes32(uint256(0x1234));
  uint64 private constant STALE_AFTER = 1 days;
  /// Receiver share of the workflow's 5,000,000 `gasLimit`; the rest is left to the forwarder.
  uint256 private constant RECEIVER_GAS_BUDGET = 4_500_000;

  // The fixture the workflow's check.ts encodes with viem; both sides pin the same keccak256.
  bytes16 private constant FIXTURE_TRANSFER_ID = 0x0e4c57f87e67585295f5418bba42563d;
  bytes32 private constant FIXTURE_REQUEST_ID = 0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef;
  bytes32 private constant FIXTURE_ROOM_REQUEST_ID = 0xfefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefe;
  bytes32 private constant FIXTURE_JOB_REQUEST_ID = 0xfafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafa;
  bytes32 private constant FIXTURE_REPORT_HASH = 0x39456b7e8cb3c9b7bce0fb55a52664dfd540dec8a39327002dd35d567079ca4e;

  uint8 private constant STANDARD = uint8(BeamTransferReceipts.TransferKind.Standard);
  uint8 private constant ROOM_DELIVERY = uint8(BeamTransferReceipts.TransferKind.RoomDelivery);
  uint8 private constant JOB = uint8(BeamTransferReceipts.TransferKind.Job);
  string private constant COMPUTE = "btr_member_cccccccccccccccccccccccccc";

  BeamTransferReceipts private receipts;
  /// Precomputed: building metadata calls the sha256 precompile, which would consume an expectRevert.
  bytes private s_metadata;

  function setUp() public {
    vm.warp(1_760_000_000);
    receipts = new BeamTransferReceipts(FORWARDER, STALE_AFTER, true);
    receipts.setRequester(REQUESTER, true);
    s_metadata = _metadata(WORKFLOW_ID, AUTHOR);
  }

  // ---------------------------------------------------------------- forwarder and workflow identity

  function testRejectsReportsNotFromForwarder() public {
    bytes memory report = _report(_standardList(_standard(bytes16(uint128(1)), bytes32(0))), _noRooms());
    bytes memory metadata = s_metadata;
    vm.expectRevert(abi.encodeWithSelector(ReceiverTemplate.InvalidSender.selector, STRANGER, FORWARDER));
    vm.prank(STRANGER);
    receipts.onReport(metadata, report);
  }

  function testAcceptsProductionMetadataWhenWorkflowIdentityMatches() public {
    receipts.setExpectedAuthor(AUTHOR);
    receipts.setExpectedWorkflowId(WORKFLOW_ID);
    receipts.setExpectedWorkflowName("transfer-receipts");
    bytes16 transferId = bytes16(uint128(1));

    _deliver(_report(_standardList(_standard(transferId, bytes32(0))), _noRooms()));

    _eq(uint8(receipts.getReceipt(transferId).status), uint8(BeamTransferReceipts.Status.Completed));
  }

  function testRejectsReportsFromAnotherWorkflowOrAuthor() public {
    receipts.setExpectedAuthor(AUTHOR);
    receipts.setExpectedWorkflowId(WORKFLOW_ID);
    bytes memory report = _report(_standardList(_standard(bytes16(uint128(1)), bytes32(0))), _noRooms());
    bytes32 otherWorkflow = bytes32(uint256(0x9999));
    bytes memory otherWorkflowMetadata = _metadata(otherWorkflow, AUTHOR);
    bytes memory otherAuthorMetadata = _metadata(WORKFLOW_ID, STRANGER);

    vm.expectRevert(abi.encodeWithSelector(ReceiverTemplate.InvalidWorkflowId.selector, otherWorkflow, WORKFLOW_ID));
    vm.prank(FORWARDER);
    receipts.onReport(otherWorkflowMetadata, report);

    vm.expectRevert(abi.encodeWithSelector(ReceiverTemplate.InvalidAuthor.selector, STRANGER, AUTHOR));
    vm.prank(FORWARDER);
    receipts.onReport(otherAuthorMetadata, report);
  }

  // ---------------------------------------------------------------- receipt kinds, versions and levels

  function testRecordsStandardReceiptWithLevel() public {
    bytes16 transferId = bytes16(uint128(7));
    BeamTransferReceipts.StandardReceiptReport memory entry = _standard(transferId, bytes32(0));
    entry.status = uint8(BeamTransferReceipts.Status.Failed);
    entry.bytesTransferred = 512;

    _deliver(_report(_standardList(entry), _noRooms()));

    BeamTransferReceipts.StandardReceipt memory receipt = receipts.getReceipt(transferId);
    _eq(uint8(receipt.status), uint8(BeamTransferReceipts.Status.Failed));
    _eq(receipt.totalBytes, entry.totalBytes);
    _eq(receipt.bytesTransferred, 512);
    _eq(receipt.completedAtMs, entry.completedAtMs);
    _eq(receipt.planFingerprint, entry.planFingerprint);
    _eq(receipt.coordinateChecksum, entry.coordinateChecksum);
    _eq(receipt.requestId, bytes32(0));
    _eq(receipt.level, receipts.standardReceiptLevel());
    _eq(receipts.standardReceiptLevel(), receipts.LEVEL_BEAM_REPORTED() | receipts.LEVEL_DON_ATTESTED());
  }

  function testRecordsRoomReceiptsWithEvidenceVersionAndLevel() public {
    BeamTransferReceipts.RoomReceiptReport memory signed = _room("room-a", "channel", "pub-1");
    BeamTransferReceipts.RoomReceiptReport memory unsigned = _room("room-a", "channel", "pub-2");
    unsigned.level = receipts.roomReceiptLevel();
    unsigned.status = uint8(BeamTransferReceipts.Status.Partial);
    BeamTransferReceipts.RoomReceiptReport[] memory rooms = new BeamTransferReceipts.RoomReceiptReport[](2);
    rooms[0] = signed;
    rooms[1] = unsigned;

    _deliver(_report(_noStandard(), rooms));

    BeamTransferReceipts.RoomReceipt memory first = receipts.getRoomReceipt(receipts.roomReceiptKey("room-a", "channel", "pub-1"));
    _eq(uint8(first.status), uint8(BeamTransferReceipts.Status.Completed));
    _eq(first.fileSizeBytes, signed.fileSizeBytes);
    _eq(first.manifestSha256, signed.manifestSha256);
    _eq(first.targetCount, 2);
    _eq(first.evidenceSha256, signed.evidenceSha256);
    _eq(first.assignmentsSha256, signed.assignmentsSha256);
    _eq(first.evidenceVersion, 1);
    _eq(first.level, receipts.roomSignedReceiptLevel());
    _eq(
      receipts.roomSignedReceiptLevel(),
      receipts.LEVEL_BEAM_REPORTED() | receipts.LEVEL_TARGET_SIGNED() | receipts.LEVEL_DON_ATTESTED()
    );

    BeamTransferReceipts.RoomReceipt memory second = receipts.getRoomReceipt(receipts.roomReceiptKey("room-a", "channel", "pub-2"));
    _eq(uint8(second.status), uint8(BeamTransferReceipts.Status.Partial));
    _eq(second.level, receipts.roomReceiptLevel());
  }

  function testRejectsUnsupportedReportVersion() public {
    _expectReportRevert(
      abi.encode(uint8(1), _noStandard(), _noRooms(), new BeamTransferReceipts.RequestClosure[](0)),
      abi.encodeWithSelector(BeamTransferReceipts.UnsupportedReportVersion.selector, uint8(1))
    );
  }

  function testRejectsLevelsAndStatusesOutsideEachKind() public {
    BeamTransferReceipts.StandardReceiptReport memory standard = _standard(bytes16(uint128(1)), bytes32(0));
    standard.level = receipts.roomSignedReceiptLevel();
    _expectReportRevert(
      _report(_standardList(standard), _noRooms()),
      abi.encodeWithSelector(BeamTransferReceipts.InvalidLevel.selector, standard.level)
    );

    standard = _standard(bytes16(uint128(1)), bytes32(0));
    standard.status = uint8(BeamTransferReceipts.Status.Partial);
    _expectReportRevert(
      _report(_standardList(standard), _noRooms()),
      abi.encodeWithSelector(BeamTransferReceipts.InvalidStatus.selector, standard.status)
    );

    BeamTransferReceipts.RoomReceiptReport memory room = _room("r", "c", "p");
    room.targetCount = 0;
    _expectReportRevert(
      _report(_noStandard(), _roomList(room)),
      abi.encodeWithSelector(BeamTransferReceipts.InvalidLevel.selector, room.level)
    );

    room = _room("r", "c", "p");
    room.level = receipts.LEVEL_DON_ATTESTED();
    _expectReportRevert(
      _report(_noStandard(), _roomList(room)),
      abi.encodeWithSelector(BeamTransferReceipts.InvalidLevel.selector, room.level)
    );

    room = _room("r", "c", "p");
    room.evidenceVersion = 0;
    _expectReportRevert(
      _report(_noStandard(), _roomList(room)), abi.encodeWithSelector(BeamTransferReceipts.InvalidEvidence.selector)
    );
  }

  // ---------------------------------------------------------------- relay deployments

  function testCreDeploymentIsDonAttestedAndRefusesRelayLevels() public {
    _check(receipts.donAttested(), "a CRE deployment is DON-attested");
    BeamTransferReceipts.StandardReceiptReport memory standard = _standard(bytes16(uint128(1)), bytes32(0));
    standard.level = receipts.LEVEL_BEAM_REPORTED();
    _expectReportRevert(
      _report(_standardList(standard), _noRooms()),
      abi.encodeWithSelector(BeamTransferReceipts.InvalidLevel.selector, standard.level)
    );
  }

  function testRelayDeploymentRecordsBeamReportedLevels() public {
    BeamTransferReceipts relay = new BeamTransferReceipts(RELAY, STALE_AFTER, false);
    _check(!relay.donAttested(), "a relay deployment is not DON-attested");
    _eq(relay.standardReceiptLevel(), relay.LEVEL_BEAM_REPORTED());
    _eq(relay.roomReceiptLevel(), relay.LEVEL_BEAM_REPORTED());
    _eq(relay.roomSignedReceiptLevel(), relay.LEVEL_BEAM_REPORTED() | relay.LEVEL_TARGET_SIGNED());

    relay.setRequester(REQUESTER, true);
    vm.prank(REQUESTER);
    relay.requestTransfer(FIXTURE_REQUEST_ID, STANDARD, "relay");
    bytes16 transferId = bytes16(uint128(9));
    BeamTransferReceipts.StandardReceiptReport memory standard = _standard(transferId, FIXTURE_REQUEST_ID);
    standard.level = relay.standardReceiptLevel();
    BeamTransferReceipts.RoomReceiptReport memory room = _room("r", "c", "p");
    room.level = relay.roomSignedReceiptLevel();

    vm.prank(RELAY);
    relay.onReport("", _report(_standardList(standard), _roomList(room)));

    _eq(relay.getReceipt(transferId).level, relay.LEVEL_BEAM_REPORTED());
    _eqId(relay.transferIdForRequest(FIXTURE_REQUEST_ID), transferId);
    _eq(relay.openRequestCount(), 0);
    _eq(
      relay.getRoomReceipt(relay.roomReceiptKey("r", "c", "p")).level,
      relay.LEVEL_BEAM_REPORTED() | relay.LEVEL_TARGET_SIGNED()
    );
  }

  function testRelayDeploymentRefusesTheDonAttestedLevels() public {
    BeamTransferReceipts relay = new BeamTransferReceipts(RELAY, STALE_AFTER, false);
    // Built with the CRE deployment's levels, which carry LEVEL_DON_ATTESTED.
    BeamTransferReceipts.StandardReceiptReport memory standard = _standard(bytes16(uint128(1)), bytes32(0));
    BeamTransferReceipts.RoomReceiptReport memory room = _room("r", "c", "p");
    bytes memory standardReport = _report(_standardList(standard), _noRooms());
    bytes memory roomReport = _report(_noStandard(), _roomList(room));

    vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.InvalidLevel.selector, standard.level));
    vm.prank(RELAY);
    relay.onReport("", standardReport);

    vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.InvalidLevel.selector, room.level));
    vm.prank(RELAY);
    relay.onReport("", roomReport);
  }

  function testRelayDeploymentAcceptsReportsOnlyFromTheRelay() public {
    BeamTransferReceipts relay = new BeamTransferReceipts(RELAY, STALE_AFTER, false);
    bytes memory report = _report(_noStandard(), _noRooms());
    vm.expectRevert(abi.encodeWithSelector(ReceiverTemplate.InvalidSender.selector, STRANGER, RELAY));
    vm.prank(STRANGER);
    relay.onReport("", report);
  }

  function testDecodesTheWorkflowEncodingOfTheSharedFixture() public {
    BeamTransferReceipts.StandardReceiptReport memory standard = BeamTransferReceipts.StandardReceiptReport({
      transferId: FIXTURE_TRANSFER_ID,
      requestId: FIXTURE_REQUEST_ID,
      status: 1,
      totalBytes: 1_073_741_824,
      bytesTransferred: 1_073_741_824,
      completedAtMs: 1_760_000_000_000,
      planFingerprint: bytes32(0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa),
      coordinateChecksum: bytes32(0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb),
      level: 5
    });
    BeamTransferReceipts.RoomReceiptReport memory room = BeamTransferReceipts.RoomReceiptReport({
      room: "room-1",
      channel: "channel-1",
      publication: "publication-1",
      requestId: FIXTURE_ROOM_REQUEST_ID,
      status: 4,
      fileSizeBytes: 5_368_709_120,
      manifestSha256: bytes32(0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc),
      targetCount: 2,
      evidenceSha256: bytes32(0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd),
      assignmentsSha256: bytes32(0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee),
      evidenceVersion: 1,
      level: 7
    });
    BeamTransferReceipts.RequestClosure[] memory closures = new BeamTransferReceipts.RequestClosure[](1);
    closures[0] = BeamTransferReceipts.RequestClosure({
      key: receipts.roomRequestKey(receipts.roomReceiptKey("room-2", "channel-2", "publication-2")),
      reason: 1,
      transferId: FIXTURE_TRANSFER_ID
    });
    BeamTransferReceipts.RoomReceiptReport[] memory rooms = new BeamTransferReceipts.RoomReceiptReport[](2);
    rooms[0] = room;
    rooms[1] = BeamTransferReceipts.RoomReceiptReport({
      room: "room-1",
      channel: "channel-out",
      publication: "publication-out",
      requestId: bytes32(0),
      status: 1,
      fileSizeBytes: 64,
      manifestSha256: bytes32(0x9999999999999999999999999999999999999999999999999999999999999999),
      targetCount: 1,
      evidenceSha256: bytes32(0x8888888888888888888888888888888888888888888888888888888888888888),
      assignmentsSha256: bytes32(0x7777777777777777777777777777777777777777777777777777777777777777),
      evidenceVersion: 1,
      level: 5
    });
    BeamTransferReceipts.JobResultReport[] memory jobs = new BeamTransferReceipts.JobResultReport[](1);
    jobs[0] = BeamTransferReceipts.JobResultReport({
      requestId: FIXTURE_JOB_REQUEST_ID,
      inputRoomKey: receipts.roomReceiptKey("room-1", "channel-1", "publication-1"),
      outputRoomKey: receipts.roomReceiptKey("room-1", "channel-out", "publication-out"),
      computeMember: COMPUTE
    });
    bytes memory report = abi.encode(uint8(2), _standardList(standard), rooms, closures, jobs);
    _eq(keccak256(report), FIXTURE_REPORT_HASH);

    _deliver(report);
    _eq(receipts.getReceipt(FIXTURE_TRANSFER_ID).totalBytes, 1_073_741_824);
    _eqId(receipts.transferIdForRequest(FIXTURE_REQUEST_ID), FIXTURE_TRANSFER_ID);
    _eq(receipts.getRoomReceipt(receipts.roomReceiptKey("room-1", "channel-1", "publication-1")).targetCount, 2);
    BeamTransferReceipts.JobResult memory fixtureJob = receipts.getJob(FIXTURE_JOB_REQUEST_ID);
    // The input is Partial, so the job is too; its level is what both receipts claim (7 & 5).
    _eq(uint8(fixtureJob.status), uint8(BeamTransferReceipts.Status.Partial));
    _eq(fixtureJob.level, 5);
    _eq(receipts.roomKeyForRequest(FIXTURE_ROOM_REQUEST_ID), receipts.roomReceiptKey("room-1", "channel-1", "publication-1"));
  }

  // ---------------------------------------------------------------- dedupe

  function testRepeatedReportIsANoOp() public {
    bytes16 transferId = bytes16(uint128(3));
    bytes memory report =
      _report(_standardList(_standard(transferId, bytes32(uint256(9)))), _roomList(_room("r", "c", "p")));
    _deliver(report);
    BeamTransferReceipts.StandardReceipt memory first = receipts.getReceipt(transferId);

    vm.recordLogs();
    _deliver(report);
    _eq(vm.getRecordedLogs().length, 0);

    BeamTransferReceipts.StandardReceipt memory second = receipts.getReceipt(transferId);
    _eq(second.requestId, first.requestId);
    _eq(second.completedAtMs, first.completedAtMs);
  }

  function testLaterReportDoesNotOverwriteARecordedReceipt() public {
    bytes16 transferId = bytes16(uint128(3));
    _deliver(_report(_standardList(_standard(transferId, bytes32(0))), _noRooms()));

    BeamTransferReceipts.StandardReceiptReport memory changed = _standard(transferId, bytes32(0));
    changed.status = uint8(BeamTransferReceipts.Status.Cancelled);
    changed.totalBytes = 1;
    _deliver(_report(_standardList(changed), _noRooms()));

    _eq(uint8(receipts.getReceipt(transferId).status), uint8(BeamTransferReceipts.Status.Completed));
    _eq(receipts.getReceipt(transferId).totalBytes, _standard(transferId, bytes32(0)).totalBytes);
  }

  function testRepeatedRequestsAreNoOps() public {
    bytes16 transferId = bytes16(uint128(5));
    vm.prank(REQUESTER);
    receipts.requestReceipt(transferId);
    vm.prank(REQUESTER);
    receipts.requestTransfer(bytes32(uint256(1)), STANDARD, "input");
    vm.prank(REQUESTER);
    receipts.requestRoomReceipt("r", "c", "p");
    _eq(receipts.openRequestCount(), 3);

    vm.recordLogs();
    vm.prank(REQUESTER);
    receipts.requestReceipt(transferId);
    vm.prank(REQUESTER);
    receipts.requestTransfer(bytes32(uint256(1)), STANDARD, "other input");
    vm.prank(REQUESTER);
    receipts.requestRoomReceipt("r", "c", "p");
    _eq(vm.getRecordedLogs().length, 0);
    _eq(receipts.openRequestCount(), 3);
  }

  function testRequestForARecordedReceiptIsANoOp() public {
    bytes16 transferId = bytes16(uint128(6));
    _deliver(_report(_standardList(_standard(transferId, bytes32(0))), _roomList(_room("r", "c", "p"))));

    vm.recordLogs();
    vm.prank(REQUESTER);
    receipts.requestReceipt(transferId);
    vm.prank(REQUESTER);
    receipts.requestRoomReceipt("r", "c", "p");
    _eq(vm.getRecordedLogs().length, 0);
    _eq(receipts.openRequestCount(), 0);
  }

  // ---------------------------------------------------------------- open-request set

  function testOpenSetGrowsAndShrinksAsReceiptsAreRecorded() public {
    bytes32 requestId = bytes32(uint256(0xabc));
    bytes16 derivedTransferId = bytes16(uint128(0x111));
    bytes16 knownTransferId = bytes16(uint128(0x222));

    vm.prank(REQUESTER);
    receipts.requestTransfer(requestId, STANDARD, "input");
    vm.prank(REQUESTER);
    receipts.requestReceipt(knownTransferId);
    vm.prank(REQUESTER);
    receipts.requestRoomReceipt("room", "channel", "publication");
    _eq(receipts.openRequestCount(), 3);

    BeamTransferReceipts.OpenRequest[] memory page = receipts.openRequests(0, 2);
    _eq(page.length, 2);
    _eq(uint8(page[0].kind), uint8(BeamTransferReceipts.RequestKind.Transfer));
    _eq(page[0].requestId, requestId);
    _eq(page[0].requester, REQUESTER);
    _eq(uint8(page[1].kind), uint8(BeamTransferReceipts.RequestKind.Receipt));
    _eqId(page[1].transferId, knownTransferId);
    page = receipts.openRequests(2, 10);
    _eq(page.length, 1);
    _eq(keccak256(bytes(page[0].publication)), keccak256("publication"));
    _eq(receipts.openRequests(3, 10).length, 0);

    BeamTransferReceipts.StandardReceiptReport[] memory standard = new BeamTransferReceipts.StandardReceiptReport[](2);
    standard[0] = _standard(derivedTransferId, requestId);
    standard[1] = _standard(knownTransferId, bytes32(0));
    _deliver(_report(standard, _noRooms()));
    _eq(receipts.openRequestCount(), 1);
    _eqId(receipts.transferIdForRequest(requestId), derivedTransferId);
    _eq(receipts.getReceipt(derivedTransferId).requestId, requestId);
    _check(!receipts.isOpen(receipts.transferRequestKey(requestId)), "transfer request still open");

    _deliver(_report(_noStandard(), _roomList(_room("room", "channel", "publication"))));
    _eq(receipts.openRequestCount(), 0);

    vm.recordLogs();
    vm.prank(REQUESTER);
    receipts.requestTransfer(requestId, STANDARD, "input");
    _eq(vm.getRecordedLogs().length, 0);
  }

  function testLateRequestIdBindsToAnEarlierReceipt() public {
    bytes32 requestId = bytes32(uint256(0xdef));
    bytes16 transferId = bytes16(uint128(0x333));
    _deliver(_report(_standardList(_standard(transferId, bytes32(0))), _noRooms()));

    vm.prank(REQUESTER);
    receipts.requestTransfer(requestId, STANDARD, "input");
    _eq(receipts.openRequestCount(), 1);

    _deliver(_report(_standardList(_standard(transferId, requestId)), _noRooms()));
    _eq(receipts.openRequestCount(), 0);
    _eqId(receipts.transferIdForRequest(requestId), transferId);
    _eq(receipts.getReceipt(transferId).requestId, requestId);
  }

  function testStaleRequestsListsOnlyRequestsOlderThanStaleAfter() public {
    uint256 start = block.timestamp;
    vm.prank(REQUESTER);
    receipts.requestReceipt(bytes16(uint128(1)));
    vm.warp(start + 12 hours);
    vm.prank(REQUESTER);
    receipts.requestReceipt(bytes16(uint128(2)));
    _eq(receipts.staleRequests().length, 0);

    vm.warp(start + STALE_AFTER);
    BeamTransferReceipts.OpenRequest[] memory stale = receipts.staleRequests();
    _eq(stale.length, 1);
    _eqId(stale[0].transferId, bytes16(uint128(1)));

    vm.warp(start + STALE_AFTER + 12 hours);
    _eq(receipts.staleRequests().length, 2);

    _deliver(_report(_standardList(_standard(bytes16(uint128(1)), bytes32(0))), _noRooms()));
    stale = receipts.staleRequests();
    _eq(stale.length, 1);
    _eqId(stale[0].transferId, bytes16(uint128(2)));

    receipts.setStaleAfter(30 days);
    _eq(receipts.staleRequests().length, 0);
  }

  function testOpenSetIsBounded() public {
    uint256 max = receipts.MAX_OPEN_REQUESTS();
    for (uint256 i = 1; i <= max; ++i) {
      vm.prank(REQUESTER);
      receipts.requestReceipt(bytes16(uint128(i)));
    }
    _eq(receipts.openRequestCount(), max);

    vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.OpenRequestsFull.selector, max));
    vm.prank(REQUESTER);
    receipts.requestReceipt(bytes16(uint128(max + 1)));

    vm.prank(REQUESTER);
    receipts.cancelRequest(receipts.receiptRequestKey(bytes16(uint128(1))));
    vm.prank(REQUESTER);
    receipts.requestReceipt(bytes16(uint128(max + 1)));
    _eq(receipts.openRequestCount(), max);
  }

  function testCancelRequest() public {
    bytes32 requestId = bytes32(uint256(77));
    bytes32 key = receipts.transferRequestKey(requestId);
    vm.prank(REQUESTER);
    receipts.requestTransfer(requestId, STANDARD, "input");

    vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.NotRequestOwner.selector, STRANGER));
    vm.prank(STRANGER);
    receipts.cancelRequest(key);

    vm.prank(REQUESTER);
    receipts.cancelRequest(key);
    _eq(receipts.openRequestCount(), 0);

    vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.UnknownRequest.selector, key));
    receipts.cancelRequest(key);

    vm.recordLogs();
    vm.prank(REQUESTER);
    receipts.requestTransfer(requestId, STANDARD, "input");
    _eq(vm.getRecordedLogs().length, 1);
    receipts.cancelRequest(key);
    _eq(receipts.openRequestCount(), 0);
  }

  /// A full batch, 14 receipts with maximum-length ids, each closing its open requests, fits the gas budget.
  function testFullReportFitsTheGasBudget() public {
    BeamTransferReceipts.RoomReceiptReport[] memory rooms = new BeamTransferReceipts.RoomReceiptReport[](14);
    BeamTransferReceipts.StandardReceiptReport[] memory standard = new BeamTransferReceipts.StandardReceiptReport[](14);
    bytes memory longId = new bytes(receipts.MAX_ROOM_ID_BYTES());
    for (uint256 i = 0; i < longId.length; ++i) {
      longId[i] = "r";
    }
    string memory room = string(abi.encodePacked(longId));
    for (uint256 i = 0; i < 14; ++i) {
      longId[0] = bytes1(uint8(65 + i));
      string memory publication = string(abi.encodePacked(longId));
      vm.prank(REQUESTER);
      receipts.requestRoomReceipt(room, room, publication);
      rooms[i] = _room(room, room, publication);
      rooms[i].requestId = bytes32(i + 1001);
      vm.prank(REQUESTER);
      receipts.requestTransfer(rooms[i].requestId, ROOM_DELIVERY, "input");
      bytes32 requestId = bytes32(i + 1);
      vm.prank(REQUESTER);
      receipts.requestTransfer(requestId, STANDARD, "input");
      standard[i] = _standard(bytes16(uint128(i + 1)), requestId);
    }
    bytes memory roomReport = _report(_noStandard(), rooms);
    bytes memory standardReport = _report(standard, _noRooms());
    bytes memory metadata = s_metadata;

    vm.prank(FORWARDER);
    uint256 before = gasleft();
    receipts.onReport(metadata, roomReport);
    uint256 roomGas = before - gasleft();
    vm.prank(FORWARDER);
    before = gasleft();
    receipts.onReport(metadata, standardReport);
    uint256 standardGas = before - gasleft();

    BeamTransferReceipts.RequestClosure[] memory closures = new BeamTransferReceipts.RequestClosure[](14);
    for (uint256 i = 0; i < 14; ++i) {
      longId[1] = bytes1(uint8(97 + i));
      string memory publication = string(abi.encodePacked(longId));
      vm.prank(REQUESTER);
      receipts.requestRoomReceipt(room, publication, publication);
      closures[i] = BeamTransferReceipts.RequestClosure({
        key: receipts.roomRequestKey(receipts.roomReceiptKey(room, publication, publication)),
        reason: 1,
        transferId: bytes16(uint128(i + 1))
      });
    }
    bytes memory closureReport = abi.encode(uint8(2), _noStandard(), _noRooms(), closures, _noJobs());
    vm.prank(FORWARDER);
    before = gasleft();
    receipts.onReport(metadata, closureReport);
    uint256 closureGas = before - gasleft();

    emit log_named_uint("gas for 14 room receipts", roomGas);
    emit log_named_uint("gas for 14 standard receipts", standardGas);
    emit log_named_uint("gas for 14 closures", closureGas);
    _eq(receipts.openRequestCount(), 0);
    _check(
      roomGas < RECEIVER_GAS_BUDGET && standardGas < RECEIVER_GAS_BUDGET && closureGas < RECEIVER_GAS_BUDGET,
      "full report exceeds the gas budget"
    );
  }

  /// Four jobs with maximum-length ids, eight Room receipts and four job results: the most a 14-entry report holds.
  function testFullJobReportFitsTheGasBudget() public {
    bytes memory longId = new bytes(receipts.MAX_ROOM_ID_BYTES());
    for (uint256 i = 0; i < longId.length; ++i) {
      longId[i] = "r";
    }
    string memory room = string(abi.encodePacked(longId));
    BeamTransferReceipts.RoomReceiptReport[] memory jobRooms = new BeamTransferReceipts.RoomReceiptReport[](8);
    BeamTransferReceipts.JobResultReport[] memory jobs = new BeamTransferReceipts.JobResultReport[](4);
    bytes memory longMember = new bytes(receipts.MAX_ROOM_ID_BYTES());
    for (uint256 i = 0; i < longMember.length; ++i) {
      longMember[i] = "m";
    }
    for (uint256 i = 0; i < 4; ++i) {
      longId[2] = bytes1(uint8(65 + i));
      string memory input = string(abi.encodePacked(longId));
      longId[3] = bytes1(uint8(65 + i));
      string memory output = string(abi.encodePacked(longId));
      jobRooms[2 * i] = _room(room, room, input);
      jobRooms[2 * i + 1] = _room(room, room, output);
      bytes32 jobId = bytes32(i + 5001);
      vm.prank(REQUESTER);
      receipts.requestTransfer(jobId, JOB, "input");
      jobs[i] = BeamTransferReceipts.JobResultReport({
        requestId: jobId,
        inputRoomKey: receipts.roomReceiptKey(room, room, input),
        outputRoomKey: receipts.roomReceiptKey(room, room, output),
        computeMember: string(longMember)
      });
    }
    bytes memory jobReport =
      abi.encode(uint8(2), _noStandard(), jobRooms, new BeamTransferReceipts.RequestClosure[](0), jobs);
    bytes memory metadata = s_metadata;
    vm.prank(FORWARDER);
    uint256 before = gasleft();
    receipts.onReport(metadata, jobReport);
    uint256 jobGas = before - gasleft();

    emit log_named_uint("gas for 4 jobs with their 8 room receipts", jobGas);
    _eq(receipts.openRequestCount(), 0);
    _check(jobGas < RECEIVER_GAS_BUDGET, "full job report exceeds the gas budget");
  }

  // ---------------------------------------------------------------- request kinds

  function testRoomReceiptAnswersARoomDeliveryRequest() public {
    bytes32 requestId = bytes32(uint256(0x5005));
    vm.prank(REQUESTER);
    receipts.requestTransfer(requestId, ROOM_DELIVERY, "input");
    BeamTransferReceipts.OpenRequest[] memory open = receipts.openRequests(0, 1);
    _eq(uint8(open[0].kind), uint8(BeamTransferReceipts.RequestKind.Transfer));
    _eq(uint8(open[0].transferKind), ROOM_DELIVERY);

    BeamTransferReceipts.RoomReceiptReport memory room = _room("room", "channel", "publication");
    room.requestId = requestId;
    _deliver(_report(_noStandard(), _roomList(room)));

    bytes32 roomKey = receipts.roomReceiptKey("room", "channel", "publication");
    _eq(receipts.openRequestCount(), 0);
    _eq(receipts.roomKeyForRequest(requestId), roomKey);
    _eqId(receipts.transferIdForRequest(requestId), bytes16(0));

    vm.recordLogs();
    vm.prank(REQUESTER);
    receipts.requestTransfer(requestId, ROOM_DELIVERY, "input");
    _eq(vm.getRecordedLogs().length, 0);
  }

  function testLateRoomRequestIdBindsToAnEarlierRoomReceipt() public {
    bytes32 requestId = bytes32(uint256(0x5006));
    _deliver(_report(_noStandard(), _roomList(_room("room", "channel", "publication"))));
    vm.prank(REQUESTER);
    receipts.requestTransfer(requestId, ROOM_DELIVERY, "input");

    BeamTransferReceipts.RoomReceiptReport memory room = _room("room", "channel", "publication");
    room.requestId = requestId;
    _deliver(_report(_noStandard(), _roomList(room)));

    _eq(receipts.openRequestCount(), 0);
    _eq(receipts.roomKeyForRequest(requestId), receipts.roomReceiptKey("room", "channel", "publication"));
  }

  function testAReceiptOfTheOtherKindLeavesARequestOpen() public {
    bytes32 roomRequest = bytes32(uint256(0x7001));
    bytes32 standardRequest = bytes32(uint256(0x7002));
    vm.prank(REQUESTER);
    receipts.requestTransfer(roomRequest, ROOM_DELIVERY, "input");
    vm.prank(REQUESTER);
    receipts.requestTransfer(standardRequest, STANDARD, "input");

    BeamTransferReceipts.RoomReceiptReport memory room = _room("room", "channel", "publication");
    room.requestId = standardRequest;
    _deliver(_report(_standardList(_standard(bytes16(uint128(1)), roomRequest)), _roomList(room)));

    _eq(receipts.openRequestCount(), 2);
    _eqId(receipts.transferIdForRequest(roomRequest), bytes16(0));
    _eq(receipts.roomKeyForRequest(standardRequest), bytes32(0));
    _eq(receipts.getReceipt(bytes16(uint128(1))).requestId, bytes32(0));
    _eq(uint8(receipts.getRoomReceipt(receipts.roomReceiptKey("room", "channel", "publication")).status), 1);
  }

  function testRoomDeliveryRequestsShowAsStaleUntilAnswered() public {
    uint256 start = block.timestamp;
    vm.prank(REQUESTER);
    receipts.requestTransfer(bytes32(uint256(0x8001)), ROOM_DELIVERY, "input");
    vm.warp(start + STALE_AFTER);
    BeamTransferReceipts.OpenRequest[] memory stale = receipts.staleRequests();
    _eq(stale.length, 1);
    _eq(uint8(stale[0].transferKind), ROOM_DELIVERY);
  }

  function testRejectsUnknownTransferKinds() public {
    vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.InvalidTransferKind.selector, uint8(0)));
    receipts.requestTransfer(bytes32(uint256(1)), 0, "input");
    vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.InvalidTransferKind.selector, uint8(4)));
    receipts.requestTransfer(bytes32(uint256(1)), 4, "input");
  }

  // ---------------------------------------------------------------- jobs

  function testJobIsAnsweredByItsResult() public {
    bytes32 jobId = bytes32(uint256(0x7001));
    vm.prank(REQUESTER);
    receipts.requestTransfer(jobId, JOB, "input");
    _eq(receipts.openRequestCount(), 1);

    BeamTransferReceipts.RoomReceiptReport[] memory rooms = new BeamTransferReceipts.RoomReceiptReport[](2);
    rooms[0] = _room("room", "inputs", "pub-in");
    rooms[1] = _room("room", "outputs", "pub-out");
    rooms[1].manifestSha256 = sha256("output manifest");
    rooms[1].level = receipts.roomReceiptLevel();
    vm.recordLogs();
    _deliver(_jobReport(rooms, _job(jobId, "pub-in", "pub-out")));

    BeamTransferReceipts.JobResult memory job = receipts.getJob(jobId);
    _eq(uint8(job.status), uint8(BeamTransferReceipts.Status.Completed));
    _eq(job.inputManifestSha256, sha256("manifest"));
    _eq(job.outputManifestSha256, sha256("output manifest"));
    _eq(job.inputRoomKey, receipts.roomReceiptKey("room", "inputs", "pub-in"));
    _eq(job.outputRoomKey, receipts.roomReceiptKey("room", "outputs", "pub-out"));
    _check(keccak256(bytes(job.computeMember)) == keccak256(bytes(COMPUTE)), "compute member");
    // The output is not target-signed, so the job keeps only the claims both receipts hold.
    _eq(job.level, receipts.roomReceiptLevel());
    _eq(receipts.openRequestCount(), 0);
    _check(!receipts.isOpen(receipts.transferRequestKey(jobId)), "job request still open");
  }

  function testJobStatusFollowsTheInputThenTheOutput() public {
    BeamTransferReceipts.RoomReceiptReport[] memory rooms = new BeamTransferReceipts.RoomReceiptReport[](2);
    rooms[0] = _room("room", "inputs", "in-a");
    rooms[1] = _room("room", "outputs", "out-a");
    rooms[1].status = uint8(BeamTransferReceipts.Status.Partial);
    _deliver(_jobReport(rooms, _job(bytes32(uint256(0x7002)), "in-a", "out-a")));
    _eq(uint8(receipts.getJob(bytes32(uint256(0x7002))).status), uint8(BeamTransferReceipts.Status.Partial));

    rooms[0] = _room("room", "inputs", "in-b");
    rooms[0].status = uint8(BeamTransferReceipts.Status.Failed);
    rooms[1] = _room("room", "outputs", "out-b");
    _deliver(_jobReport(rooms, _job(bytes32(uint256(0x7003)), "in-b", "out-b")));
    _eq(uint8(receipts.getJob(bytes32(uint256(0x7003))).status), uint8(BeamTransferReceipts.Status.Failed));
  }

  function testJobNeedsBothRoomReceipts() public {
    BeamTransferReceipts.RoomReceiptReport[] memory rooms = new BeamTransferReceipts.RoomReceiptReport[](1);
    rooms[0] = _room("room", "inputs", "pub-in");
    _expectReportRevert(
      _jobReport(rooms, _job(bytes32(uint256(0x7004)), "pub-in", "missing")),
      abi.encodeWithSelector(
        BeamTransferReceipts.MissingRoomReceipt.selector, receipts.roomReceiptKey("room", "outputs", "missing")
      )
    );
  }

  function testRejectsMalformedJobs() public {
    BeamTransferReceipts.RoomReceiptReport[] memory rooms = new BeamTransferReceipts.RoomReceiptReport[](2);
    rooms[0] = _room("room", "inputs", "pub-in");
    rooms[1] = _room("room", "outputs", "pub-out");
    BeamTransferReceipts.JobResultReport memory sameKeys = _job(bytes32(uint256(0x7005)), "pub-in", "pub-out");
    sameKeys.outputRoomKey = sameKeys.inputRoomKey;
    _expectReportRevert(_jobReport(rooms, sameKeys), abi.encodeWithSelector(BeamTransferReceipts.InvalidJob.selector));
    BeamTransferReceipts.JobResultReport memory noMember = _job(bytes32(uint256(0x7005)), "pub-in", "pub-out");
    noMember.computeMember = "";
    _expectReportRevert(_jobReport(rooms, noMember), abi.encodeWithSelector(BeamTransferReceipts.InvalidJob.selector));
    _expectReportRevert(
      _jobReport(rooms, _job(bytes32(0), "pub-in", "pub-out")), abi.encodeWithSelector(BeamTransferReceipts.InvalidJob.selector)
    );
  }

  function testRepeatedJobReportIsANoOp() public {
    bytes32 jobId = bytes32(uint256(0x7006));
    BeamTransferReceipts.RoomReceiptReport[] memory rooms = new BeamTransferReceipts.RoomReceiptReport[](2);
    rooms[0] = _room("room", "inputs", "pub-in");
    rooms[1] = _room("room", "outputs", "pub-out");
    bytes memory report = _jobReport(rooms, _job(jobId, "pub-in", "pub-out"));
    _deliver(report);
    vm.recordLogs();
    _deliver(report);
    _eq(vm.getRecordedLogs().length, 0);
    _eq(uint8(receipts.getJob(jobId).status), uint8(BeamTransferReceipts.Status.Completed));
  }

  function testJobResultDoesNotAnswerARoomDeliveryRequest() public {
    bytes32 requestId = bytes32(uint256(0x7007));
    vm.prank(REQUESTER);
    receipts.requestTransfer(requestId, ROOM_DELIVERY, "input");
    BeamTransferReceipts.RoomReceiptReport[] memory rooms = new BeamTransferReceipts.RoomReceiptReport[](2);
    rooms[0] = _room("room", "inputs", "pub-in");
    rooms[1] = _room("room", "outputs", "pub-out");
    _deliver(_jobReport(rooms, _job(requestId, "pub-in", "pub-out")));
    _eq(uint8(receipts.getJob(requestId).status), uint8(BeamTransferReceipts.Status.None));
    _check(receipts.isOpen(receipts.transferRequestKey(requestId)), "room delivery request was closed");
  }

  // ---------------------------------------------------------------- closures

  function testBucketOnlyClosureClosesTheRoomRequest() public {
    vm.prank(REQUESTER);
    receipts.requestRoomReceipt("room", "channel", "publication");
    bytes32 key = receipts.roomRequestKey(receipts.roomReceiptKey("room", "channel", "publication"));
    bytes16 transferId = bytes16(uint128(0x9001));

    vm.recordLogs();
    _deliver(_closureReport(key, receipts.CLOSURE_BUCKET_ONLY_PUBLICATION(), transferId));
    Vm.Log[] memory logs = vm.getRecordedLogs();

    _eq(receipts.openRequestCount(), 0);
    _eq(uint8(receipts.getRoomReceipt(receipts.roomReceiptKey("room", "channel", "publication")).status), 0);
    _eq(logs.length, 1);
    _eq(logs[0].topics[0], keccak256("RequestClosed(bytes32,uint8,bytes16)"));
    _eq(logs[0].topics[1], key);
    (uint8 reason, bytes16 named) = abi.decode(logs[0].data, (uint8, bytes16));
    _eq(reason, 1);
    _eqId(named, transferId);

    vm.prank(REQUESTER);
    receipts.requestReceipt(named);
    _eq(receipts.openRequestCount(), 1);
  }

  function testRepeatedOrUnknownClosuresAreNoOps() public {
    vm.prank(REQUESTER);
    receipts.requestRoomReceipt("room", "channel", "publication");
    bytes memory report =
      _closureReport(receipts.roomRequestKey(receipts.roomReceiptKey("room", "channel", "publication")), 1, bytes16(0));
    _deliver(report);

    vm.recordLogs();
    _deliver(report);
    _deliver(_closureReport(keccak256("never opened"), 1, bytes16(0)));
    _eq(vm.getRecordedLogs().length, 0);
  }

  function testClosuresNeverTouchOtherRequestKinds() public {
    bytes16 transferId = bytes16(uint128(0x9002));
    vm.prank(REQUESTER);
    receipts.requestReceipt(transferId);
    vm.prank(REQUESTER);
    receipts.requestTransfer(bytes32(uint256(0x9003)), STANDARD, "input");
    vm.prank(REQUESTER);
    receipts.requestTransfer(bytes32(uint256(0x9004)), ROOM_DELIVERY, "input");

    vm.recordLogs();
    _deliver(_closureReport(receipts.receiptRequestKey(transferId), 1, transferId));
    _deliver(_closureReport(receipts.transferRequestKey(bytes32(uint256(0x9003))), 1, transferId));
    _deliver(_closureReport(receipts.transferRequestKey(bytes32(uint256(0x9004))), 1, transferId));
    _eq(vm.getRecordedLogs().length, 0);
    _eq(receipts.openRequestCount(), 3);
  }

  function testRejectsUnknownClosureReasonsAndZeroKeys() public {
    bytes32 key = receipts.roomRequestKey(receipts.roomReceiptKey("room", "channel", "publication"));
    _expectReportRevert(
      _closureReport(key, 2, bytes16(0)), abi.encodeWithSelector(BeamTransferReceipts.InvalidClosureReason.selector, 2)
    );
    _expectReportRevert(
      _closureReport(key, 0, bytes16(0)), abi.encodeWithSelector(BeamTransferReceipts.InvalidClosureReason.selector, 0)
    );
    _expectReportRevert(
      _closureReport(bytes32(0), 1, bytes16(0)), abi.encodeWithSelector(BeamTransferReceipts.InvalidRequestKey.selector)
    );
  }

  // ---------------------------------------------------------------- access and input bounds

  function testOnlyRequestersOpenRequests() public {
    vm.expectRevert(abi.encodeWithSelector(BeamTransferReceipts.NotRequester.selector, STRANGER));
    vm.prank(STRANGER);
    receipts.requestReceipt(bytes16(uint128(1)));

    vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, STRANGER));
    vm.prank(STRANGER);
    receipts.setRequester(STRANGER, true);

    receipts.setRequester(STRANGER, true);
    _check(receipts.isRequester(STRANGER), "requester not allowed");
    vm.prank(STRANGER);
    receipts.requestReceipt(bytes16(uint128(1)));

    receipts.requestReceipt(bytes16(uint128(2)));
    _eq(receipts.openRequestCount(), 2);
  }

  function testRejectsMalformedRequests() public {
    vm.expectRevert(BeamTransferReceipts.InvalidRequestId.selector);
    receipts.requestTransfer(bytes32(0), STANDARD, "input");

    bytes memory input = new bytes(receipts.MAX_INPUT_BYTES() + 1);
    vm.expectRevert(
      abi.encodeWithSelector(BeamTransferReceipts.InputTooLarge.selector, input.length, receipts.MAX_INPUT_BYTES())
    );
    receipts.requestTransfer(bytes32(uint256(1)), STANDARD, string(input));

    vm.expectRevert(BeamTransferReceipts.InvalidTransferId.selector);
    receipts.requestReceipt(bytes16(0));

    vm.expectRevert(BeamTransferReceipts.InvalidRoomId.selector);
    receipts.requestRoomReceipt("", "c", "p");

    string memory longId = string(new bytes(receipts.MAX_ROOM_ID_BYTES() + 1));
    vm.expectRevert(BeamTransferReceipts.InvalidRoomId.selector);
    receipts.requestRoomReceipt("r", longId, "p");

    vm.expectRevert(BeamTransferReceipts.InvalidStaleAfter.selector);
    receipts.setStaleAfter(0);
  }

  function testRoomReceiptKeyIsTheAbiEncodedTriple() public view {
    _eq(receipts.roomReceiptKey("r", "c", "p"), keccak256(abi.encode("r", "c", "p")));
    _check(
      receipts.roomReceiptKey("ab", "c", "p") != receipts.roomReceiptKey("a", "bc", "p"), "room keys must not collide"
    );
  }

  // ---------------------------------------------------------------- helpers

  function _standard(bytes16 transferId, bytes32 requestId)
    private
    view
    returns (BeamTransferReceipts.StandardReceiptReport memory)
  {
    return BeamTransferReceipts.StandardReceiptReport({
      transferId: transferId,
      requestId: requestId,
      status: uint8(BeamTransferReceipts.Status.Completed),
      totalBytes: 1 << 30,
      bytesTransferred: 1 << 30,
      completedAtMs: 1_760_000_000_000,
      planFingerprint: keccak256("plan"),
      coordinateChecksum: keccak256("coordinates"),
      level: receipts.standardReceiptLevel()
    });
  }

  function _room(string memory room, string memory channel, string memory publication)
    private
    view
    returns (BeamTransferReceipts.RoomReceiptReport memory)
  {
    return BeamTransferReceipts.RoomReceiptReport({
      room: room,
      channel: channel,
      publication: publication,
      requestId: bytes32(0),
      status: uint8(BeamTransferReceipts.Status.Completed),
      fileSizeBytes: 5 << 30,
      manifestSha256: sha256("manifest"),
      targetCount: 2,
      evidenceSha256: sha256("evidence"),
      assignmentsSha256: sha256("assignments"),
      evidenceVersion: 1,
      level: receipts.roomSignedReceiptLevel()
    });
  }

  function _standardList(BeamTransferReceipts.StandardReceiptReport memory entry)
    private
    pure
    returns (BeamTransferReceipts.StandardReceiptReport[] memory list)
  {
    list = new BeamTransferReceipts.StandardReceiptReport[](1);
    list[0] = entry;
  }

  function _roomList(BeamTransferReceipts.RoomReceiptReport memory entry)
    private
    pure
    returns (BeamTransferReceipts.RoomReceiptReport[] memory list)
  {
    list = new BeamTransferReceipts.RoomReceiptReport[](1);
    list[0] = entry;
  }

  function _noStandard() private pure returns (BeamTransferReceipts.StandardReceiptReport[] memory) {
    return new BeamTransferReceipts.StandardReceiptReport[](0);
  }

  function _noRooms() private pure returns (BeamTransferReceipts.RoomReceiptReport[] memory) {
    return new BeamTransferReceipts.RoomReceiptReport[](0);
  }

  function _noJobs() private pure returns (BeamTransferReceipts.JobResultReport[] memory) {
    return new BeamTransferReceipts.JobResultReport[](0);
  }

  function _report(
    BeamTransferReceipts.StandardReceiptReport[] memory standard,
    BeamTransferReceipts.RoomReceiptReport[] memory rooms
  ) private pure returns (bytes memory) {
    return abi.encode(uint8(2), standard, rooms, new BeamTransferReceipts.RequestClosure[](0), _noJobs());
  }

  function _closureReport(bytes32 key, uint8 reason, bytes16 transferId) private pure returns (bytes memory) {
    BeamTransferReceipts.RequestClosure[] memory closures = new BeamTransferReceipts.RequestClosure[](1);
    closures[0] = BeamTransferReceipts.RequestClosure({key: key, reason: reason, transferId: transferId});
    return abi.encode(uint8(2), _noStandard(), _noRooms(), closures, _noJobs());
  }

  /// A job whose input is published in channel "inputs" and output in "outputs" of Room "room".
  function _job(bytes32 requestId, string memory input, string memory output)
    private
    view
    returns (BeamTransferReceipts.JobResultReport memory)
  {
    return BeamTransferReceipts.JobResultReport({
      requestId: requestId,
      inputRoomKey: receipts.roomReceiptKey("room", "inputs", input),
      outputRoomKey: receipts.roomReceiptKey("room", "outputs", output),
      computeMember: COMPUTE
    });
  }

  function _jobReport(BeamTransferReceipts.RoomReceiptReport[] memory rooms, BeamTransferReceipts.JobResultReport memory job)
    private
    pure
    returns (bytes memory)
  {
    BeamTransferReceipts.JobResultReport[] memory jobs = new BeamTransferReceipts.JobResultReport[](1);
    jobs[0] = job;
    return abi.encode(uint8(2), _noStandard(), rooms, new BeamTransferReceipts.RequestClosure[](0), jobs);
  }

  /// The 64-byte metadata a production KeystoneForwarder passes: id, name, owner, then a 2-byte report id.
  function _metadata(bytes32 workflowId, address owner) private pure returns (bytes memory) {
    return abi.encodePacked(workflowId, _workflowName("transfer-receipts"), owner, bytes2(0));
  }

  function _deliver(bytes memory report) private {
    vm.prank(FORWARDER);
    receipts.onReport(s_metadata, report);
  }

  function _expectReportRevert(bytes memory report, bytes memory revertData) private {
    bytes memory metadata = s_metadata;
    vm.expectRevert(revertData);
    vm.prank(FORWARDER);
    receipts.onReport(metadata, report);
  }

  /// `ReceiverTemplate.setExpectedWorkflowName`'s encoding: the first 10 hex characters of sha256(name).
  function _workflowName(string memory name) private pure returns (bytes10) {
    bytes32 hash = sha256(bytes(name));
    bytes16 hexChars = "0123456789abcdef";
    bytes memory first10 = new bytes(10);
    for (uint256 i = 0; i < 5; ++i) {
      first10[i * 2] = hexChars[uint8(hash[i] >> 4)];
      first10[i * 2 + 1] = hexChars[uint8(hash[i] & 0x0f)];
    }
    return bytes10(first10);
  }

  function _check(bool condition, string memory message) private pure {
    if (!condition) revert(message);
  }

  function _eq(uint256 actual, uint256 expected) private pure {
    if (actual != expected) revert("uint mismatch");
  }

  function _eq(bytes32 actual, bytes32 expected) private pure {
    if (actual != expected) revert("bytes32 mismatch");
  }

  function _eqId(bytes16 actual, bytes16 expected) private pure {
    if (actual != expected) revert("bytes16 mismatch");
  }

  function _eq(address actual, address expected) private pure {
    if (actual != expected) revert("address mismatch");
  }
}
