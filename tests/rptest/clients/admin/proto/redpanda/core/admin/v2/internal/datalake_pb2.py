"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/v2/internal/datalake.proto')
_sym_db = _symbol_database.Default()
from .......proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from .......proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n4proto/redpanda/core/admin/v2/internal/datalake.proto\x12\x1fredpanda.core.admin.v2.internal\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto"3\n\x1aGetCoordinatorStateRequest\x12\x15\n\rtopics_filter\x18\x01 \x03(\t"_\n\x1bGetCoordinatorStateResponse\x12@\n\x05state\x18\x01 \x01(\x0b21.redpanda.core.admin.v2.internal.CoordinatorState"\xcd\x01\n\x10CoordinatorState\x12X\n\x0ctopic_states\x18\x01 \x03(\x0b2B.redpanda.core.admin.v2.internal.CoordinatorState.TopicStatesEntry\x1a_\n\x10TopicStatesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12:\n\x05value\x18\x02 \x01(\x0b2+.redpanda.core.admin.v2.internal.TopicState:\x028\x01"\x96\x01\n\x08DataFile\x12\x13\n\x0bremote_path\x18\x01 \x01(\t\x12\x11\n\trow_count\x18\x02 \x01(\x04\x12\x17\n\x0ffile_size_bytes\x18\x03 \x01(\x04\x12\x17\n\x0ftable_schema_id\x18\x04 \x01(\x05\x12\x19\n\x11partition_spec_id\x18\x05 \x01(\x05\x12\x15\n\rpartition_key\x18\x06 \x03(\x0c"\xde\x01\n\x15TranslatedOffsetRange\x12\x14\n\x0cstart_offset\x18\x01 \x01(\x03\x12\x13\n\x0blast_offset\x18\x02 \x01(\x03\x12=\n\ndata_files\x18\x03 \x03(\x0b2).redpanda.core.admin.v2.internal.DataFile\x12<\n\tdlq_files\x18\x04 \x03(\x0b2).redpanda.core.admin.v2.internal.DataFile\x12\x1d\n\x15kafka_processed_bytes\x18\x05 \x01(\x04"n\n\x0cPendingEntry\x12D\n\x04data\x18\x01 \x01(\x0b26.redpanda.core.admin.v2.internal.TranslatedOffsetRange\x12\x18\n\x10added_pending_at\x18\x02 \x01(\x03"\x88\x01\n\x0ePartitionState\x12F\n\x0fpending_entries\x18\x01 \x03(\x0b2-.redpanda.core.admin.v2.internal.PendingEntry\x12\x1b\n\x0elast_committed\x18\x02 \x01(\x03H\x00\x88\x01\x01B\x11\n\x0f_last_committed"\xd2\x02\n\nTopicState\x12\x10\n\x08revision\x18\x01 \x01(\x03\x12Z\n\x10partition_states\x18\x02 \x03(\x0b2@.redpanda.core.admin.v2.internal.TopicState.PartitionStatesEntry\x12H\n\x0flifecycle_state\x18\x03 \x01(\x0e2/.redpanda.core.admin.v2.internal.LifecycleState\x12#\n\x1btotal_kafka_processed_bytes\x18\x04 \x01(\x04\x1ag\n\x14PartitionStatesEntry\x12\x0b\n\x03key\x18\x01 \x01(\x05\x12>\n\x05value\x18\x02 \x01(\x0b2/.redpanda.core.admin.v2.internal.PartitionState:\x028\x01*\x83\x01\n\x0eLifecycleState\x12\x1f\n\x1bLIFECYCLE_STATE_UNSPECIFIED\x10\x00\x12\x18\n\x14LIFECYCLE_STATE_LIVE\x10\x01\x12\x1a\n\x16LIFECYCLE_STATE_CLOSED\x10\x02\x12\x1a\n\x16LIFECYCLE_STATE_PURGED\x10\x032\xac\x01\n\x0fDatalakeService\x12\x98\x01\n\x13GetCoordinatorState\x12;.redpanda.core.admin.v2.internal.GetCoordinatorStateRequest\x1a<.redpanda.core.admin.v2.internal.GetCoordinatorStateResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.v2.internal.datalake_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_COORDINATORSTATE_TOPICSTATESENTRY']._loaded_options = None
    _globals['_COORDINATORSTATE_TOPICSTATESENTRY']._serialized_options = b'8\x01'
    _globals['_TOPICSTATE_PARTITIONSTATESENTRY']._loaded_options = None
    _globals['_TOPICSTATE_PARTITIONSTATESENTRY']._serialized_options = b'8\x01'
    _globals['_DATALAKESERVICE'].methods_by_name['GetCoordinatorState']._loaded_options = None
    _globals['_DATALAKESERVICE'].methods_by_name['GetCoordinatorState']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LIFECYCLESTATE']._serialized_start = 1496
    _globals['_LIFECYCLESTATE']._serialized_end = 1627
    _globals['_GETCOORDINATORSTATEREQUEST']._serialized_start = 167
    _globals['_GETCOORDINATORSTATEREQUEST']._serialized_end = 218
    _globals['_GETCOORDINATORSTATERESPONSE']._serialized_start = 220
    _globals['_GETCOORDINATORSTATERESPONSE']._serialized_end = 315
    _globals['_COORDINATORSTATE']._serialized_start = 318
    _globals['_COORDINATORSTATE']._serialized_end = 523
    _globals['_COORDINATORSTATE_TOPICSTATESENTRY']._serialized_start = 428
    _globals['_COORDINATORSTATE_TOPICSTATESENTRY']._serialized_end = 523
    _globals['_DATAFILE']._serialized_start = 526
    _globals['_DATAFILE']._serialized_end = 676
    _globals['_TRANSLATEDOFFSETRANGE']._serialized_start = 679
    _globals['_TRANSLATEDOFFSETRANGE']._serialized_end = 901
    _globals['_PENDINGENTRY']._serialized_start = 903
    _globals['_PENDINGENTRY']._serialized_end = 1013
    _globals['_PARTITIONSTATE']._serialized_start = 1016
    _globals['_PARTITIONSTATE']._serialized_end = 1152
    _globals['_TOPICSTATE']._serialized_start = 1155
    _globals['_TOPICSTATE']._serialized_end = 1493
    _globals['_TOPICSTATE_PARTITIONSTATESENTRY']._serialized_start = 1390
    _globals['_TOPICSTATE_PARTITIONSTATESENTRY']._serialized_end = 1493
    _globals['_DATALAKESERVICE']._serialized_start = 1630
    _globals['_DATALAKESERVICE']._serialized_end = 1802