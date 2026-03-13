import { request as nodeHttpRequest } from "node:http";
import { createHash, randomBytes } from "node:crypto";
import { EventEmitter } from "node:events";
import { connect as nodeNetConnect, type Socket as NodeNetSocket } from "node:net";

export type IncusRecord = Record<string, unknown>;

export type IncusInstanceType = "container" | "virtual-machine" | string;

export type IncusBinaryInput =
  | Blob
  | ArrayBuffer
  | ArrayBufferView
  | Uint8Array
  | ReadableStream<Uint8Array>;

export type IncusProgress = {
  processedBytes?: number;
  totalBytes?: number;
  percent?: number;
  speedBytesPerSecond?: number;
};

export type IncusEntity<T = unknown> = {
  value: T;
  etag?: string;
};

export type IncusMutationOptions = {
  etag?: string;
};

export type IncusListOptions = {
  filter?: string[];
  allProjects?: boolean;
};

export type IncusScopeOptions = {
  allProjects?: boolean;
};

export type IncusRequestContext = {
  project?: string;
  target?: string;
  requireAuthenticated?: boolean;
};

export type IncusConnectionOptions = {
  userAgent?: string;
  authType?: string;
  skipGetEvents?: boolean;
  skipGetServer?: boolean;
  headers?: HeadersInit;
  fetch?: typeof fetch;
  signal?: AbortSignal;
  tempPath?: string;
  tlsServerCert?: string;
  tlsClientCert?: string;
  tlsClientKey?: string;
  tlsCA?: string;
  insecureSkipVerify?: boolean;
  identicalCertificate?: boolean;
  oidcTokens?: {
    accessToken?: string;
    refreshToken?: string;
    expiry?: string;
  };
};

export type IncusSimpleStreamsOptions = IncusConnectionOptions & {
  cachePath?: string;
  cacheExpiryMs?: number;
};

export type IncusUnixConnectOptions = IncusConnectionOptions & {
  socketPath?: string;
};

export type IncusHttpConnectOptions = IncusConnectionOptions & {
  endpoint?: string;
};

export type IncusConnectKind =
  | "incus"
  | "incus-public"
  | "incus-unix"
  | "incus-http"
  | "simple-streams";

export type IncusConnectionInfo = {
  addresses: string[];
  certificate?: string;
  protocol?: string;
  url?: string;
  socketPath?: string;
  project?: string;
  target?: string;
};

export type IncusTransportRequestOptions = {
  query?: Record<string, string | number | boolean | undefined>;
  headers?: HeadersInit;
  body?: unknown;
  signal?: AbortSignal;
  etag?: string;
  context?: IncusRequestContext;
};

export type IncusTransportResponse<T = unknown> = {
  status: number;
  etag?: string;
  data: T;
  headers?: Headers;
};

export interface IncusTransport {
  request<T = unknown>(
    method: string,
    path: string,
    options?: IncusTransportRequestOptions,
  ): Promise<IncusTransportResponse<T>>;
  websocket?(path: string, protocols?: string | string[]): Promise<WebSocket>;
  close?(): void;
}

export type IncusOperationWaitOptions = {
  timeoutSeconds?: number;
  signal?: AbortSignal;
};

export interface IncusOperation {
  id: string;
  wait(options?: IncusOperationWaitOptions): Promise<IncusRecord>;
  cancel(): Promise<void>;
  refresh(): Promise<IncusRecord>;
  websocket(secret: string): Promise<WebSocket>;
  onUpdate(handler: (operation: IncusRecord) => void): Promise<() => void>;
}

export interface IncusAwaitableOperation extends IncusOperation, PromiseLike<IncusRecord> {
  catch<TResult = never>(
    onRejected?: ((reason: unknown) => TResult | PromiseLike<TResult>) | null,
  ): Promise<IncusRecord | TResult>;
  finally(onFinally?: (() => void) | null): Promise<IncusRecord>;
}

export interface IncusRemoteOperation {
  wait(options?: IncusOperationWaitOptions): Promise<void>;
  cancelTarget(): Promise<void>;
  target(): Promise<IncusRecord | null>;
  onUpdate(handler: (operation: IncusRecord) => void): Promise<() => void>;
}

export type IncusEventStreamOptions = {
  types?: string[];
  allProjects?: boolean;
  signal?: AbortSignal;
};

export type IncusEvent = {
  type: string;
  timestamp?: string;
  metadata?: IncusRecord;
};

export interface IncusEventListener extends AsyncIterable<IncusEvent> {
  close(): Promise<void>;
}

export interface ConnectionApi {
  info(): Promise<IncusConnectionInfo>;
  httpClient(): Promise<unknown>;
  doHttp(request: Request): Promise<Response>;
  disconnect(): void;
}

export interface ServerApi {
  metrics(): Promise<string>;
  get(): Promise<IncusEntity<IncusRecord>>;
  resources(): Promise<IncusRecord>;
  update(server: IncusRecord, options?: IncusMutationOptions): Promise<void>;
  applyPreseed(config: IncusRecord): Promise<void>;
  hasExtension(extension: string): Promise<boolean>;
  isClustered(): Promise<boolean>;
}

export interface CertificatesApi {
  fingerprints(): Promise<string[]>;
  list(options?: IncusListOptions): Promise<IncusRecord[]>;
  get(fingerprint: string): Promise<IncusEntity<IncusRecord>>;
  create(certificate: IncusRecord): Promise<void>;
  update(
    fingerprint: string,
    certificate: IncusRecord,
    options?: IncusMutationOptions,
  ): Promise<void>;
  remove(fingerprint: string): Promise<void>;
  createToken(certificate: IncusRecord): IncusAwaitableOperation;
}

export type ImageListOptions = IncusListOptions;

export type ImageDownloadOptions = {
  signal?: AbortSignal;
  onProgress?: (progress: IncusProgress) => void;
  secret?: string;
};

export type ImageDownloadResult = {
  metadataName?: string;
  metadataSize?: number;
  rootfsName?: string;
  rootfsSize?: number;
};

export interface ImageAliasesApi {
  list(): Promise<IncusRecord[]>;
  names(): Promise<string[]>;
  get(name: string, options?: { imageType?: string }): Promise<IncusEntity<IncusRecord>>;
  getArchitectures(
    name: string,
    options?: { imageType?: string },
  ): Promise<Record<string, IncusRecord>>;
  create(alias: IncusRecord): Promise<void>;
  update(name: string, alias: IncusRecord, options?: IncusMutationOptions): Promise<void>;
  rename(name: string, alias: IncusRecord): Promise<void>;
  remove(name: string): Promise<void>;
}

export interface ImagesApi {
  list(options?: ImageListOptions): Promise<IncusRecord[]>;
  fingerprints(options?: ImageListOptions): Promise<string[]>;
  get(
    fingerprint: string,
    options?: {
      secret?: string;
    },
  ): Promise<IncusEntity<IncusRecord>>;
  downloadFile(
    fingerprint: string,
    options?: ImageDownloadOptions,
  ): Promise<ImageDownloadResult>;
  create(image: IncusRecord, upload?: IncusRecord): IncusAwaitableOperation;
  copyFrom(
    source: IncusImageClient,
    image: IncusRecord,
    options?: IncusRecord,
  ): Promise<IncusRemoteOperation>;
  update(
    fingerprint: string,
    image: IncusRecord,
    options?: IncusMutationOptions,
  ): Promise<void>;
  remove(fingerprint: string): IncusAwaitableOperation;
  refresh(fingerprint: string): IncusAwaitableOperation;
  createSecret(fingerprint: string): IncusAwaitableOperation;
  export(fingerprint: string, request?: IncusRecord): IncusAwaitableOperation;
  aliases: ImageAliasesApi;
}

export type InstanceListOptions = IncusListOptions & {
  type?: IncusInstanceType;
  full?: boolean;
};

export type InstanceExecOptions = {
  stdin?: IncusBinaryInput;
  stdout?: "pipe" | WritableStream<Uint8Array>;
  stderr?: "pipe" | WritableStream<Uint8Array>;
  signal?: AbortSignal;
  onControl?: (socket: WebSocket) => void;
};

export type InstanceForkOptions = {
  fromSnapshot?: string;
  sourceProject?: string;
  live?: boolean;
  instanceOnly?: boolean;
  refresh?: boolean;
  refreshExcludeOlder?: boolean;
  allowInconsistent?: boolean;
};

export type IncusExecOutputChunk = {
  stream: "stdout" | "stderr";
  chunk: Uint8Array;
};

export type IncusExecResult = {
  operation: IncusRecord;
  exitCode?: number;
  ok: boolean;
};

export interface IncusExecProcess extends
  IncusOperation,
  AsyncIterable<Uint8Array>,
  PromiseLike<IncusExecResult>
{
  stdout: AsyncIterable<Uint8Array>;
  stderr: AsyncIterable<Uint8Array>;
  output(): AsyncIterable<IncusExecOutputChunk>;
  waitResult(options?: IncusOperationWaitOptions): Promise<IncusExecResult>;
  catch<TResult = never>(
    onRejected?: ((reason: unknown) => TResult | PromiseLike<TResult>) | null,
  ): Promise<IncusExecResult | TResult>;
  finally(onFinally?: (() => void) | null): Promise<IncusExecResult>;
}

export type InstanceConsoleOptions = {
  terminal?: WebSocket;
  signal?: AbortSignal;
  onControl?: (socket: WebSocket) => void;
};

export type InstanceConsoleDynamicResult = {
  operation: IncusOperation;
  attach: (terminal: WebSocket) => Promise<void>;
};

export type InstanceFilePutOptions = {
  content: IncusBinaryInput;
  uid?: number;
  gid?: number;
  mode?: number;
  type?: "file" | "directory" | string;
  writeMode?: "overwrite" | "append" | string;
};

export type InstanceFileResult = {
  stream: ReadableStream<Uint8Array>;
  uid?: number;
  gid?: number;
  mode?: number;
  type?: string;
  entries?: string[];
};

export interface InstanceLogsApi {
  list(): Promise<string[]>;
  get(filename: string): Promise<ReadableStream<Uint8Array>>;
  remove(filename: string): Promise<void>;
  getConsole(): Promise<ReadableStream<Uint8Array>>;
  removeConsole(): Promise<void>;
}

export interface InstanceFilesApi {
  get(path: string): Promise<InstanceFileResult>;
  put(path: string, options: InstanceFilePutOptions): Promise<void>;
  remove(path: string): Promise<void>;
  sftp(): Promise<unknown>;
}

export interface InstanceTemplatesApi {
  list(): Promise<string[]>;
  get(templateName: string): Promise<ReadableStream<Uint8Array>>;
  put(templateName: string, content: IncusBinaryInput): Promise<void>;
  remove(templateName: string): Promise<void>;
}

export interface InstanceSnapshotsApi {
  names(): Promise<string[]>;
  list(): Promise<IncusRecord[]>;
  get(name: string): Promise<IncusEntity<IncusRecord>>;
  create(snapshot: IncusRecord): IncusAwaitableOperation;
  copyFrom(
    source: IncusClient,
    snapshot: IncusRecord,
    options?: IncusRecord,
  ): Promise<IncusRemoteOperation>;
  rename(name: string, request: IncusRecord): IncusAwaitableOperation;
  migrate(name: string, request: IncusRecord): IncusAwaitableOperation;
  remove(name: string): IncusAwaitableOperation;
  update(
    name: string,
    snapshot: IncusRecord,
    options?: IncusMutationOptions,
  ): IncusAwaitableOperation;
  restore(name: string, options?: { stateful?: boolean }): IncusAwaitableOperation;
}

export interface InstanceBackupsApi {
  names(): Promise<string[]>;
  list(): Promise<IncusRecord[]>;
  get(name: string): Promise<IncusEntity<IncusRecord>>;
  create(backup: IncusRecord): IncusAwaitableOperation;
  rename(name: string, backup: IncusRecord): IncusAwaitableOperation;
  remove(name: string): IncusAwaitableOperation;
  download(name: string): Promise<ReadableStream<Uint8Array>>;
  upload(backup: IncusRecord, body: IncusBinaryInput): Promise<void>;
}

export interface InstanceApi {
  readonly name: string;
  get(options?: { full?: boolean }): Promise<IncusEntity<IncusRecord>>;
  fork(name: string, options?: InstanceForkOptions): IncusAwaitableOperation;
  update(instance: IncusRecord, options?: IncusMutationOptions): IncusAwaitableOperation;
  rename(request: IncusRecord): IncusAwaitableOperation;
  migrate(request: IncusRecord): IncusAwaitableOperation;
  remove(): IncusAwaitableOperation;
  rebuild(request: IncusRecord): IncusAwaitableOperation;
  rebuildFromImage(
    source: IncusImageClient,
    image: IncusRecord,
    request: IncusRecord,
  ): Promise<IncusRemoteOperation>;
  restore(snapshotName: string, options?: { stateful?: boolean }): IncusAwaitableOperation;
  state(): Promise<IncusEntity<IncusRecord>>;
  setState(state: IncusRecord, options?: IncusMutationOptions): IncusAwaitableOperation;
  access(): Promise<IncusRecord>;
  exec(request: IncusRecord, options?: InstanceExecOptions): IncusExecProcess;
  console(
    request: IncusRecord,
    options?: InstanceConsoleOptions,
  ): IncusAwaitableOperation;
  consoleDynamic(
    request: IncusRecord,
    options?: InstanceConsoleOptions,
  ): Promise<InstanceConsoleDynamicResult>;
  metadata(): Promise<IncusEntity<IncusRecord>>;
  updateMetadata(metadata: IncusRecord, options?: IncusMutationOptions): Promise<void>;
  debugMemory(format?: string): Promise<ReadableStream<Uint8Array>>;
  logs: InstanceLogsApi;
  files: InstanceFilesApi;
  templates: InstanceTemplatesApi;
  snapshots: InstanceSnapshotsApi;
  backups: InstanceBackupsApi;
}

export interface InstancesApi {
  names(options?: InstanceListOptions): Promise<string[] | Record<string, string[]>>;
  list(options?: InstanceListOptions): Promise<IncusRecord[]>;
  instance(name: string): InstanceApi;
  create(instance: IncusRecord): IncusAwaitableOperation;
  createFromImage(
    source: IncusImageClient,
    image: IncusRecord,
    request: IncusRecord,
  ): Promise<IncusRemoteOperation>;
  createFromBackup(args: IncusRecord): IncusAwaitableOperation;
  copyFrom(
    source: IncusClient,
    instance: IncusRecord,
    options?: IncusRecord,
  ): Promise<IncusRemoteOperation>;
  updateMany(state: IncusRecord, options?: IncusMutationOptions): IncusAwaitableOperation;
}

export interface EventsApi {
  stream(options?: IncusEventStreamOptions): Promise<IncusEventListener>;
  send(event: IncusEvent): Promise<void>;
}

export interface MetadataApi {
  configuration(): Promise<IncusRecord>;
}

export interface NetworkForwardsApi {
  addresses(networkName: string): Promise<string[]>;
  list(networkName: string): Promise<IncusRecord[]>;
  get(networkName: string, listenAddress: string): Promise<IncusEntity<IncusRecord>>;
  create(networkName: string, request: IncusRecord): Promise<void>;
  update(
    networkName: string,
    listenAddress: string,
    request: IncusRecord,
    options?: IncusMutationOptions,
  ): Promise<void>;
  remove(networkName: string, listenAddress: string): Promise<void>;
}

export interface NetworkLoadBalancersApi {
  addresses(networkName: string): Promise<string[]>;
  list(networkName: string): Promise<IncusRecord[]>;
  get(networkName: string, listenAddress: string): Promise<IncusEntity<IncusRecord>>;
  state(networkName: string, listenAddress: string): Promise<IncusRecord>;
  create(networkName: string, request: IncusRecord): Promise<void>;
  update(
    networkName: string,
    listenAddress: string,
    request: IncusRecord,
    options?: IncusMutationOptions,
  ): Promise<void>;
  remove(networkName: string, listenAddress: string): Promise<void>;
}

export interface NetworkPeersApi {
  names(networkName: string): Promise<string[]>;
  list(networkName: string): Promise<IncusRecord[]>;
  get(networkName: string, peerName: string): Promise<IncusEntity<IncusRecord>>;
  create(networkName: string, request: IncusRecord): Promise<void>;
  update(
    networkName: string,
    peerName: string,
    request: IncusRecord,
    options?: IncusMutationOptions,
  ): Promise<void>;
  remove(networkName: string, peerName: string): Promise<void>;
}

export interface NetworkAclsApi {
  names(): Promise<string[]>;
  list(options?: IncusScopeOptions): Promise<IncusRecord[]>;
  get(name: string): Promise<IncusEntity<IncusRecord>>;
  getLog(name: string): Promise<ReadableStream<Uint8Array>>;
  create(request: IncusRecord): Promise<void>;
  update(name: string, request: IncusRecord, options?: IncusMutationOptions): Promise<void>;
  rename(name: string, request: IncusRecord): Promise<void>;
  remove(name: string): Promise<void>;
}

export interface NetworkAddressSetsApi {
  names(): Promise<string[]>;
  list(options?: IncusScopeOptions): Promise<IncusRecord[]>;
  get(name: string): Promise<IncusEntity<IncusRecord>>;
  create(request: IncusRecord): Promise<void>;
  update(name: string, request: IncusRecord, options?: IncusMutationOptions): Promise<void>;
  rename(name: string, request: IncusRecord): Promise<void>;
  remove(name: string): Promise<void>;
}

export interface NetworkZoneRecordsApi {
  names(zone: string): Promise<string[]>;
  list(zone: string): Promise<IncusRecord[]>;
  get(zone: string, name: string): Promise<IncusEntity<IncusRecord>>;
  create(zone: string, request: IncusRecord): Promise<void>;
  update(
    zone: string,
    name: string,
    request: IncusRecord,
    options?: IncusMutationOptions,
  ): Promise<void>;
  remove(zone: string, name: string): Promise<void>;
}

export interface NetworkZonesApi {
  names(): Promise<string[]>;
  list(options?: IncusScopeOptions): Promise<IncusRecord[]>;
  get(name: string): Promise<IncusEntity<IncusRecord>>;
  create(request: IncusRecord): Promise<void>;
  update(name: string, request: IncusRecord, options?: IncusMutationOptions): Promise<void>;
  remove(name: string): Promise<void>;
  records: NetworkZoneRecordsApi;
}

export interface NetworkIntegrationsApi {
  names(): Promise<string[]>;
  list(): Promise<IncusRecord[]>;
  get(name: string): Promise<IncusEntity<IncusRecord>>;
  create(request: IncusRecord): Promise<void>;
  update(name: string, request: IncusRecord, options?: IncusMutationOptions): Promise<void>;
  rename(name: string, request: IncusRecord): Promise<void>;
  remove(name: string): Promise<void>;
}

export interface NetworksApi {
  names(): Promise<string[]>;
  list(options?: IncusListOptions): Promise<IncusRecord[]>;
  get(name: string): Promise<IncusEntity<IncusRecord>>;
  leases(name: string): Promise<IncusRecord[]>;
  state(name: string): Promise<IncusRecord>;
  create(request: IncusRecord): Promise<void>;
  update(name: string, request: IncusRecord, options?: IncusMutationOptions): Promise<void>;
  rename(name: string, request: IncusRecord): Promise<void>;
  remove(name: string): Promise<void>;
  allocations(options?: IncusScopeOptions): Promise<IncusRecord[]>;
  forwards: NetworkForwardsApi;
  loadBalancers: NetworkLoadBalancersApi;
  peers: NetworkPeersApi;
  acls: NetworkAclsApi;
  addressSets: NetworkAddressSetsApi;
  zones: NetworkZonesApi;
  integrations: NetworkIntegrationsApi;
}

export interface OperationsApi {
  uuids(): Promise<string[]>;
  list(options?: IncusScopeOptions): Promise<IncusRecord[]>;
  get(uuid: string): Promise<IncusEntity<IncusRecord>>;
  wait(uuid: string, timeoutSeconds?: number): Promise<IncusEntity<IncusRecord>>;
  waitWithSecret(
    uuid: string,
    secret: string,
    timeoutSeconds?: number,
  ): Promise<IncusEntity<IncusRecord>>;
  websocket(uuid: string, secret: string): Promise<WebSocket>;
  remove(uuid: string): Promise<void>;
}

export interface ProfilesApi {
  names(): Promise<string[]>;
  list(options?: IncusListOptions): Promise<IncusRecord[]>;
  get(name: string): Promise<IncusEntity<IncusRecord>>;
  create(profile: IncusRecord): Promise<void>;
  update(name: string, profile: IncusRecord, options?: IncusMutationOptions): Promise<void>;
  rename(name: string, profile: IncusRecord): Promise<void>;
  remove(name: string): Promise<void>;
}

export interface ProjectsApi {
  names(): Promise<string[]>;
  list(options?: IncusListOptions): Promise<IncusRecord[]>;
  get(name: string): Promise<IncusEntity<IncusRecord>>;
  state(name: string): Promise<IncusRecord>;
  access(name: string): Promise<IncusRecord>;
  create(project: IncusRecord): Promise<void>;
  update(name: string, project: IncusRecord, options?: IncusMutationOptions): Promise<void>;
  rename(name: string, project: IncusRecord): IncusAwaitableOperation;
  remove(name: string): Promise<void>;
  removeForce(name: string): Promise<void>;
}

export interface StoragePoolsApi {
  names(): Promise<string[]>;
  list(options?: IncusListOptions): Promise<IncusRecord[]>;
  get(name: string): Promise<IncusEntity<IncusRecord>>;
  resources(name: string): Promise<IncusRecord>;
  create(pool: IncusRecord): Promise<void>;
  update(name: string, pool: IncusRecord, options?: IncusMutationOptions): Promise<void>;
  remove(name: string): Promise<void>;
}

export interface StorageBucketKeysApi {
  names(poolName: string, bucketName: string): Promise<string[]>;
  list(poolName: string, bucketName: string): Promise<IncusRecord[]>;
  get(
    poolName: string,
    bucketName: string,
    keyName: string,
  ): Promise<IncusEntity<IncusRecord>>;
  create(poolName: string, bucketName: string, key: IncusRecord): Promise<IncusRecord>;
  update(
    poolName: string,
    bucketName: string,
    keyName: string,
    key: IncusRecord,
    options?: IncusMutationOptions,
  ): Promise<void>;
  remove(poolName: string, bucketName: string, keyName: string): Promise<void>;
}

export interface StorageBucketBackupsApi {
  create(poolName: string, bucketName: string, backup: IncusRecord): IncusAwaitableOperation;
  remove(poolName: string, bucketName: string, name: string): IncusAwaitableOperation;
  download(poolName: string, bucketName: string, name: string): Promise<ReadableStream<Uint8Array>>;
  upload(
    poolName: string,
    bucketName: string,
    backup: IncusRecord,
    body: IncusBinaryInput,
  ): Promise<void>;
  createFromBackup(poolName: string, args: IncusRecord): IncusAwaitableOperation;
}

export interface StorageBucketsApi {
  names(poolName: string): Promise<string[]>;
  list(poolName: string, options?: IncusListOptions & { full?: boolean }): Promise<IncusRecord[]>;
  get(
    poolName: string,
    bucketName: string,
    options?: {
      full?: boolean;
    },
  ): Promise<IncusEntity<IncusRecord>>;
  create(poolName: string, bucket: IncusRecord): Promise<IncusRecord>;
  update(
    poolName: string,
    bucketName: string,
    bucket: IncusRecord,
    options?: IncusMutationOptions,
  ): Promise<void>;
  remove(poolName: string, bucketName: string): Promise<void>;
  keys: StorageBucketKeysApi;
  backups: StorageBucketBackupsApi;
}

export interface StorageVolumeFilesApi {
  get(
    poolName: string,
    volumeType: string,
    volumeName: string,
    filePath: string,
  ): Promise<InstanceFileResult>;
  put(
    poolName: string,
    volumeType: string,
    volumeName: string,
    filePath: string,
    options: InstanceFilePutOptions,
  ): Promise<void>;
  remove(
    poolName: string,
    volumeType: string,
    volumeName: string,
    filePath: string,
  ): Promise<void>;
}

export interface StorageVolumeSnapshotsApi {
  names(poolName: string, volumeType: string, volumeName: string): Promise<string[]>;
  list(poolName: string, volumeType: string, volumeName: string): Promise<IncusRecord[]>;
  get(
    poolName: string,
    volumeType: string,
    volumeName: string,
    snapshotName: string,
  ): Promise<IncusEntity<IncusRecord>>;
  create(
    poolName: string,
    volumeType: string,
    volumeName: string,
    request: IncusRecord,
  ): IncusAwaitableOperation;
  rename(
    poolName: string,
    volumeType: string,
    volumeName: string,
    snapshotName: string,
    request: IncusRecord,
  ): IncusAwaitableOperation;
  update(
    poolName: string,
    volumeType: string,
    volumeName: string,
    snapshotName: string,
    request: IncusRecord,
    options?: IncusMutationOptions,
  ): Promise<void>;
  remove(
    poolName: string,
    volumeType: string,
    volumeName: string,
    snapshotName: string,
  ): IncusAwaitableOperation;
}

export interface StorageVolumeBackupsApi {
  names(poolName: string, volumeName: string): Promise<string[]>;
  list(poolName: string, volumeName: string): Promise<IncusRecord[]>;
  get(poolName: string, volumeName: string, backupName: string): Promise<IncusEntity<IncusRecord>>;
  create(poolName: string, volumeName: string, backup: IncusRecord): IncusAwaitableOperation;
  rename(
    poolName: string,
    volumeName: string,
    backupName: string,
    request: IncusRecord,
  ): IncusAwaitableOperation;
  remove(poolName: string, volumeName: string, backupName: string): IncusAwaitableOperation;
  download(
    poolName: string,
    volumeName: string,
    backupName: string,
  ): Promise<ReadableStream<Uint8Array>>;
  upload(
    poolName: string,
    volumeName: string,
    backup: IncusRecord,
    body: IncusBinaryInput,
  ): Promise<void>;
  createFromBackup(poolName: string, args: IncusRecord): IncusAwaitableOperation;
  createFromIso(poolName: string, args: IncusRecord): IncusAwaitableOperation;
}

export interface StorageVolumesApi {
  names(
    poolName: string,
    options?: IncusScopeOptions,
  ): Promise<string[] | Record<string, string[]>>;
  list(
    poolName: string,
    options?: IncusListOptions & {
      full?: boolean;
    },
  ): Promise<IncusRecord[]>;
  get(
    poolName: string,
    volumeType: string,
    name: string,
    options?: {
      full?: boolean;
    },
  ): Promise<IncusEntity<IncusRecord>>;
  state(poolName: string, volumeType: string, name: string): Promise<IncusRecord>;
  create(poolName: string, volume: IncusRecord): Promise<void>;
  update(
    poolName: string,
    volumeType: string,
    name: string,
    volume: IncusRecord,
    options?: IncusMutationOptions,
  ): Promise<void>;
  remove(poolName: string, volumeType: string, name: string): Promise<void>;
  rename(poolName: string, volumeType: string, name: string, request: IncusRecord): Promise<void>;
  copyFrom(
    poolName: string,
    source: IncusClient,
    sourcePool: string,
    volume: IncusRecord,
    options?: IncusRecord,
  ): Promise<IncusRemoteOperation>;
  moveFrom(
    poolName: string,
    source: IncusClient,
    sourcePool: string,
    volume: IncusRecord,
    options?: IncusRecord,
  ): Promise<IncusRemoteOperation>;
  migrate(poolName: string, volume: IncusRecord): IncusAwaitableOperation;
  createFromMigration(poolName: string, volume: IncusRecord): IncusAwaitableOperation;
  snapshots: StorageVolumeSnapshotsApi;
  backups: StorageVolumeBackupsApi;
  files: StorageVolumeFilesApi;
  sftp(poolName: string, volumeType: string, volumeName: string): Promise<unknown>;
}

export interface StorageApi {
  pools: StoragePoolsApi;
  buckets: StorageBucketsApi;
  volumes: StorageVolumesApi;
}

export interface ClusterMembersApi {
  names(): Promise<string[]>;
  list(options?: IncusListOptions): Promise<IncusRecord[]>;
  get(name: string): Promise<IncusEntity<IncusRecord>>;
  state(name: string): Promise<IncusEntity<IncusRecord>>;
  create(member: IncusRecord): IncusAwaitableOperation;
  update(name: string, member: IncusRecord, options?: IncusMutationOptions): Promise<void>;
  rename(name: string, request: IncusRecord): Promise<void>;
  remove(name: string, options?: { force?: boolean; pending?: boolean }): Promise<void>;
  updateState(name: string, state: IncusRecord): IncusAwaitableOperation;
}

export interface ClusterGroupsApi {
  names(): Promise<string[]>;
  list(): Promise<IncusRecord[]>;
  get(name: string): Promise<IncusEntity<IncusRecord>>;
  create(group: IncusRecord): Promise<void>;
  update(name: string, group: IncusRecord, options?: IncusMutationOptions): Promise<void>;
  rename(name: string, request: IncusRecord): Promise<void>;
  remove(name: string): Promise<void>;
}

export interface ClusterApi {
  get(): Promise<IncusEntity<IncusRecord>>;
  update(cluster: IncusRecord, options?: IncusMutationOptions): IncusAwaitableOperation;
  updateCertificate(certificate: IncusRecord, options?: IncusMutationOptions): Promise<void>;
  members: ClusterMembersApi;
  groups: ClusterGroupsApi;
}

export interface WarningsApi {
  uuids(): Promise<string[]>;
  list(): Promise<IncusRecord[]>;
  get(uuid: string): Promise<IncusEntity<IncusRecord>>;
  update(uuid: string, warning: IncusRecord, options?: IncusMutationOptions): Promise<void>;
  remove(uuid: string): Promise<void>;
}

export interface OidcApi {
  tokens(): Promise<IncusRecord | null>;
}

export interface RawApi {
  query<T = IncusRecord>(
    method: string,
    path: string,
    body?: unknown,
    options?: IncusMutationOptions,
  ): Promise<IncusEntity<T>>;
  websocket(path: string): Promise<WebSocket>;
  operation(
    method: string,
    path: string,
    body?: unknown,
    options?: IncusMutationOptions,
  ): IncusAwaitableOperation;
}

type IncusTransportDescriptor = {
  kind: IncusConnectKind;
  endpoint: string;
  socketPath?: string;
};

type IncusApiEnvelope = {
  type?: string;
  status?: string;
  status_code?: number;
  operation?: string;
  error?: string;
  error_code?: number;
  metadata?: unknown;
};

type InternalRequestOptions = Omit<IncusTransportRequestOptions, "context"> & {
  applyContext?: boolean;
};

type IncusEnvelopeResult<T = unknown> = {
  value: T;
  etag?: string;
  operation?: string;
  status: number;
  headers?: Headers;
};

type FileHeaders = {
  uid?: number;
  gid?: number;
  mode?: number;
  type?: string;
};

type WebSocketLike = {
  close(code?: number, reason?: string): void;
  send(data: unknown): void;
  addEventListener?: (type: string, listener: (event: unknown) => void) => void;
  removeEventListener?: (type: string, listener: (event: unknown) => void) => void;
  on?: (type: string, listener: (...args: unknown[]) => void) => void;
  off?: (type: string, listener: (...args: unknown[]) => void) => void;
};

export class IncusApiError extends Error {
  readonly status: number;
  readonly statusCode?: number;
  readonly errorCode?: number;
  readonly details?: unknown;

  constructor(
    message: string,
    status: number,
    options: {
      statusCode?: number;
      errorCode?: number;
      details?: unknown;
    } = {},
  ) {
    super(message);
    this.name = "IncusApiError";
    this.status = status;
    this.statusCode = options.statusCode;
    this.errorCode = options.errorCode;
    this.details = options.details;
  }
}

const WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const WS_CONNECTING = 0;
const WS_OPEN = 1;
const WS_CLOSING = 2;
const WS_CLOSED = 3;

class UnixSocketWebSocket implements WebSocketLike {
  readonly CONNECTING = WS_CONNECTING;
  readonly OPEN = WS_OPEN;
  readonly CLOSING = WS_CLOSING;
  readonly CLOSED = WS_CLOSED;

  binaryType: BinaryType = "arraybuffer";
  extensions = "";
  protocol = "";
  readyState = WS_CONNECTING;
  bufferedAmount = 0;

  private readonly events = new EventEmitter();
  private readBuffer = Buffer.alloc(0);
  private fragments: Buffer[] = [];
  private fragmentOpcode: number | null = null;
  private closeInfo: { code: number; reason: string } | null = null;
  private closeEmitted = false;

  constructor(
    private readonly socket: NodeNetSocket,
    initialData?: Uint8Array,
  ) {
    this.socket.on("data", (chunk: Buffer | string) => {
      this.ingestData(chunk);
    });
    this.socket.on("error", (error: Error) => {
      this.events.emit("error", error);
    });
    this.socket.on("close", () => {
      this.readyState = WS_CLOSED;
      if (!this.closeEmitted) {
        this.closeEmitted = true;
        const info = this.closeInfo ?? { code: 1006, reason: "" };
        this.events.emit("close", info.code, info.reason);
      }
    });

    this.readyState = WS_OPEN;
    if (initialData && initialData.byteLength > 0) {
      this.ingestData(Buffer.from(initialData));
    }
  }

  addEventListener(type: string, listener: (event: unknown) => void): void {
    this.events.on(type, listener as (...args: unknown[]) => void);
  }

  removeEventListener(type: string, listener: (event: unknown) => void): void {
    this.events.off(type, listener as (...args: unknown[]) => void);
  }

  on(type: string, listener: (...args: unknown[]) => void): void {
    this.events.on(type, listener);
  }

  off(type: string, listener: (...args: unknown[]) => void): void {
    this.events.off(type, listener);
  }

  once(type: string, listener: (...args: unknown[]) => void): void {
    this.events.once(type, listener);
  }

  send(data: unknown): void {
    if (this.readyState !== WS_OPEN) {
      throw new Error("[Incus.ts] Cannot send on a closed websocket");
    }

    const payload = typeof data === "string"
      ? Buffer.from(data, "utf8")
      : Buffer.from(toBytes(data));
    const opcode = typeof data === "string" ? 0x1 : 0x2;
    this.writeFrame(opcode, payload, true);
  }

  close(code = 1000, reason = ""): void {
    if (this.readyState === WS_CLOSED) {
      return;
    }

    if (this.readyState === WS_OPEN) {
      this.readyState = WS_CLOSING;
      const reasonBytes = Buffer.from(reason, "utf8");
      const payload = Buffer.alloc(2 + reasonBytes.length);
      payload.writeUInt16BE(code, 0);
      reasonBytes.copy(payload, 2);
      this.writeFrame(0x8, payload, true);
    }

    this.socket.end();
  }

  private ingestData(chunk: Buffer | string): void {
    const bytes = typeof chunk === "string" ? Buffer.from(chunk, "utf8") : chunk;
    if (bytes.byteLength === 0) {
      return;
    }

    this.readBuffer = this.readBuffer.byteLength === 0
      ? Buffer.from(bytes)
      : Buffer.concat([this.readBuffer, bytes]);
    this.parseFrames();
  }

  private parseFrames(): void {
    while (this.readBuffer.byteLength >= 2) {
      const first = this.readBuffer[0];
      const second = this.readBuffer[1];
      if (first === undefined || second === undefined) {
        return;
      }

      const fin = (first & 0x80) !== 0;
      let opcode = first & 0x0f;
      const masked = (second & 0x80) !== 0;

      let offset = 2;
      let payloadLength = second & 0x7f;

      if (payloadLength === 126) {
        if (this.readBuffer.byteLength < offset + 2) {
          return;
        }

        payloadLength = this.readBuffer.readUInt16BE(offset);
        offset += 2;
      } else if (payloadLength === 127) {
        if (this.readBuffer.byteLength < offset + 8) {
          return;
        }

        const extended = this.readBuffer.readBigUInt64BE(offset);
        if (extended > BigInt(Number.MAX_SAFE_INTEGER)) {
          this.events.emit("error", new Error("[Incus.ts] Websocket frame too large"));
          this.close(1009, "Frame too large");
          return;
        }

        payloadLength = Number(extended);
        offset += 8;
      }

      let maskKey: Buffer | undefined;
      if (masked) {
        if (this.readBuffer.byteLength < offset + 4) {
          return;
        }

        maskKey = this.readBuffer.subarray(offset, offset + 4);
        offset += 4;
      }

      if (this.readBuffer.byteLength < offset + payloadLength) {
        return;
      }

      let payload = this.readBuffer.subarray(offset, offset + payloadLength);
      this.readBuffer = this.readBuffer.subarray(offset + payloadLength);

      if (maskKey) {
        const decoded = Buffer.alloc(payload.byteLength);
        for (let i = 0; i < payload.byteLength; i += 1) {
          decoded[i] = payload[i]! ^ maskKey[i % 4]!;
        }

        payload = decoded;
      }

      if (!fin) {
        if (opcode !== 0x0) {
          this.fragmentOpcode = opcode;
        }

        this.fragments.push(Buffer.from(payload));
        continue;
      }

      if (opcode === 0x0 && this.fragmentOpcode !== null) {
        this.fragments.push(Buffer.from(payload));
        payload = Buffer.concat(this.fragments);
        opcode = this.fragmentOpcode;
        this.fragments = [];
        this.fragmentOpcode = null;
      }

      this.handleFrame(opcode, payload);
    }
  }

  private handleFrame(opcode: number, payload: Buffer): void {
    switch (opcode) {
      case 0x1:
        this.events.emit("message", payload.toString("utf8"));
        break;
      case 0x2:
        this.events.emit("message", new Uint8Array(payload));
        break;
      case 0x8: {
        let code = 1000;
        let reason = "";
        if (payload.byteLength >= 2) {
          code = payload.readUInt16BE(0);
          reason = payload.subarray(2).toString("utf8");
        }

        this.closeInfo = { code, reason };
        if (this.readyState === WS_OPEN) {
          this.readyState = WS_CLOSING;
          this.writeFrame(0x8, payload, true);
        }

        this.socket.end();
        break;
      }
      case 0x9:
        this.writeFrame(0xA, payload, true);
        break;
      case 0xA:
        break;
      default:
        this.events.emit("error", new Error(`[Incus.ts] Unsupported websocket opcode: ${opcode}`));
    }
  }

  private writeFrame(opcode: number, payload: Buffer, mask: boolean): void {
    const parts: Buffer[] = [];
    const firstByte = 0x80 | (opcode & 0x0f);
    const payloadLength = payload.byteLength;

    if (payloadLength < 126) {
      parts.push(Buffer.from([firstByte, (mask ? 0x80 : 0) | payloadLength]));
    } else if (payloadLength <= 0xffff) {
      const header = Buffer.alloc(4);
      header[0] = firstByte;
      header[1] = (mask ? 0x80 : 0) | 126;
      header.writeUInt16BE(payloadLength, 2);
      parts.push(header);
    } else {
      const header = Buffer.alloc(10);
      header[0] = firstByte;
      header[1] = (mask ? 0x80 : 0) | 127;
      header.writeBigUInt64BE(BigInt(payloadLength), 2);
      parts.push(header);
    }

    if (mask) {
      const maskKey = randomBytes(4);
      const maskedPayload = Buffer.from(payload);
      for (let i = 0; i < maskedPayload.byteLength; i += 1) {
        maskedPayload[i] = maskedPayload[i]! ^ maskKey[i % 4]!;
      }

      parts.push(maskKey, maskedPayload);
    } else {
      parts.push(payload);
    }

    this.socket.write(Buffer.concat(parts));
  }
}

class FetchTransport implements IncusTransport {
  constructor(
    private readonly descriptor: IncusTransportDescriptor,
    private readonly defaults: Readonly<IncusConnectionOptions>,
  ) {}

  async request<T = unknown>(
    method: string,
    path: string,
    options: IncusTransportRequestOptions = {},
  ): Promise<IncusTransportResponse<T>> {
    const url = this.makeRequestUrl(path, options.query);
    const headers = this.makeHeaders(options);
    const bodyInfo = prepareBody(options.body);

    if (bodyInfo.contentType && !headers.has("Content-Type")) {
      headers.set("Content-Type", bodyInfo.contentType);
    }

    const fetchImpl = this.defaults.fetch ?? fetch;
    const response = await fetchImpl(url, {
      method: method.toUpperCase(),
      headers,
      body: bodyInfo.body,
      signal: options.signal ?? this.defaults.signal,
      redirect: "follow",
    });

    return {
      status: response.status,
      etag: response.headers.get("etag") ?? undefined,
      headers: response.headers,
      data: (await parseResponseBody(response)) as T,
    };
  }

  async websocket(path: string, protocols?: string | string[]): Promise<WebSocket> {
    const baseUrl = toWsBaseURL(this.descriptor.endpoint);
    const target = path.startsWith("ws://") || path.startsWith("wss://")
      ? path
      : new URL(normalizePath(path), baseUrl).toString();

    return new WebSocket(target, protocols);
  }

  private makeRequestUrl(
    path: string,
    query?: Record<string, string | number | boolean | undefined>,
  ): string {
    const isAbsolute = /^https?:\/\//.test(path);
    const url = isAbsolute
      ? new URL(path)
      : new URL(normalizePath(path), `${this.descriptor.endpoint}/`);

    if (query) {
      for (const [key, rawValue] of Object.entries(query)) {
        if (rawValue === undefined) {
          continue;
        }

        url.searchParams.set(key, String(rawValue));
      }
    }

    return url.toString();
  }

  private makeHeaders(options: IncusTransportRequestOptions): Headers {
    const headers = new Headers(this.defaults.headers);
    applyHeaders(headers, options.headers);

    if (this.defaults.userAgent && !headers.has("User-Agent")) {
      headers.set("User-Agent", this.defaults.userAgent);
    }

    if (options.etag) {
      headers.set("If-Match", options.etag);
    }

    if (options.context?.requireAuthenticated) {
      headers.set("X-Incus-authenticated", "true");
    }

    if (this.defaults.oidcTokens?.accessToken && !headers.has("Authorization")) {
      headers.set("Authorization", `Bearer ${this.defaults.oidcTokens.accessToken}`);
    }

    return headers;
  }
}

class UnixSocketTransport implements IncusTransport {
  constructor(
    private readonly descriptor: IncusTransportDescriptor,
    private readonly defaults: Readonly<IncusConnectionOptions>,
  ) {}

  async request<T = unknown>(
    method: string,
    path: string,
    options: IncusTransportRequestOptions = {},
  ): Promise<IncusTransportResponse<T>> {
    const socketPath = this.descriptor.socketPath;
    if (!socketPath) {
      throw new Error("[Incus.ts] Missing Unix socket path");
    }

    const requestPath = this.makeRequestPath(path, options.query);
    const headers = this.makeHeaders(options);
    const bodyInfo = await prepareUnixBody(options.body);

    if (bodyInfo.contentType && !headers.has("Content-Type")) {
      headers.set("Content-Type", bodyInfo.contentType);
    }

    if (bodyInfo.bodyBytes) {
      headers.set("Content-Length", String(bodyInfo.bodyBytes.byteLength));
    }

    return new Promise<IncusTransportResponse<T>>((resolve, reject) => {
      const req = nodeHttpRequest(
        {
          socketPath,
          path: requestPath,
          method: method.toUpperCase(),
          headers: headersToObject(headers),
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on("data", (chunk: Buffer | string) => {
            if (typeof chunk === "string") {
              chunks.push(Buffer.from(chunk));
            } else {
              chunks.push(chunk);
            }
          });
          res.on("end", () => {
            const bytes = Buffer.concat(chunks);
            const responseHeaders = nodeHeadersToWebHeaders(res.headers);
            const contentType = responseHeaders.get("content-type") ?? undefined;
            const parsed = parseRawBody(contentType, bytes);

            resolve({
              status: res.statusCode ?? 0,
              etag: responseHeaders.get("etag") ?? undefined,
              headers: responseHeaders,
              data: parsed as T,
            });
          });
        },
      );

      req.on("error", reject);

      if (options.signal) {
        options.signal.addEventListener(
          "abort",
          () => {
            req.destroy(new Error("Request aborted"));
          },
          { once: true },
        );
      }

      if (bodyInfo.bodyBytes) {
        req.write(bodyInfo.bodyBytes);
      }

      req.end();
    });
  }

  async websocket(path: string, protocols?: string | string[]): Promise<WebSocket> {
    const socketPath = this.descriptor.socketPath;
    if (!socketPath) {
      throw new Error("[Incus.ts] Missing Unix socket path");
    }

    const requestPath = path.startsWith("ws://") || path.startsWith("wss://")
      ? (() => {
        const url = new URL(path);
        return `${url.pathname}${url.search}`;
      })()
      : normalizePath(path);
    const headers = this.makeWebsocketHeaders();
    if (!headers.has("Host")) {
      headers.set("Host", "incus.local");
    }

    const websocketKey = randomBytes(16).toString("base64");
    headers.set("Connection", "Upgrade");
    headers.set("Upgrade", "websocket");
    headers.set("Sec-WebSocket-Version", "13");
    headers.set("Sec-WebSocket-Key", websocketKey);

    if (protocols) {
      const selected = Array.isArray(protocols) ? protocols.join(", ") : protocols;
      if (selected.length > 0) {
        headers.set("Sec-WebSocket-Protocol", selected);
      }
    }

    const requestLines = [`GET ${requestPath} HTTP/1.1`];
    for (const [key, value] of headers.entries()) {
      requestLines.push(`${key}: ${value}`);
    }
    requestLines.push("", "");

    return new Promise<WebSocket>((resolve, reject) => {
      const socket = nodeNetConnect(socketPath);
      let handshakeBuffer = Buffer.alloc(0);
      let settled = false;

      const finish = (
        handler: (socket: NodeNetSocket, tail: Uint8Array) => void,
        tail: Uint8Array,
      ) => {
        if (settled) {
          return;
        }

        settled = true;
        cleanup();
        handler(socket, tail);
      };

      const fail = (error: Error) => {
        if (settled) {
          return;
        }

        settled = true;
        cleanup();
        socket.destroy();
        reject(error);
      };

      const timeout = setTimeout(() => {
        fail(new Error("[Incus.ts] WebSocket opening handshake timed out"));
      }, 10_000);

      const cleanup = () => {
        clearTimeout(timeout);
        socket.off("connect", onConnect);
        socket.off("data", onData);
        socket.off("error", onError);
        socket.off("close", onClose);
      };

      const onConnect = () => {
        socket.write(requestLines.join("\r\n"));
      };

      const onData = (chunk: Buffer | string) => {
        const bytes = typeof chunk === "string" ? Buffer.from(chunk, "utf8") : chunk;
        handshakeBuffer = handshakeBuffer.byteLength === 0
          ? Buffer.from(bytes)
          : Buffer.concat([handshakeBuffer, bytes]);

        const delimiterIndex = handshakeBuffer.indexOf("\r\n\r\n");
        if (delimiterIndex < 0) {
          return;
        }

        const responseHeaders = handshakeBuffer.subarray(0, delimiterIndex + 4);
        const leftover = handshakeBuffer.subarray(delimiterIndex + 4);
        const responseText = responseHeaders.toString("utf8");
        const lines = responseText.split("\r\n");

        const statusLine = lines.shift() ?? "";
        const statusMatch = /^HTTP\/1\.[01]\s+(\d+)/.exec(statusLine);
        const status = statusMatch ? Number.parseInt(statusMatch[1]!, 10) : 0;
        if (status !== 101) {
          fail(new Error(`[Incus.ts] WebSocket upgrade failed with status ${status || "unknown"}`));
          return;
        }

        const parsedHeaders = new Map<string, string>();
        for (const line of lines) {
          if (line.length === 0) {
            continue;
          }

          const separator = line.indexOf(":");
          if (separator < 0) {
            continue;
          }

          const key = line.slice(0, separator).trim().toLowerCase();
          const value = line.slice(separator + 1).trim();
          parsedHeaders.set(key, value);
        }

        const upgradeHeader = parsedHeaders.get("upgrade")?.toLowerCase();
        if (upgradeHeader !== "websocket") {
          fail(new Error("[Incus.ts] Invalid websocket upgrade response"));
          return;
        }

        const accept = parsedHeaders.get("sec-websocket-accept");
        const expectedAccept = createHash("sha1")
          .update(`${websocketKey}${WEBSOCKET_GUID}`)
          .digest("base64");
        if (accept !== expectedAccept) {
          fail(new Error("[Incus.ts] Invalid websocket accept token"));
          return;
        }

        finish(
          (upgradedSocket, tail) => {
            const websocket = new UnixSocketWebSocket(
              upgradedSocket,
              tail.byteLength > 0 ? tail : undefined,
            );
            resolve(websocket as unknown as WebSocket);
          },
          new Uint8Array(leftover),
        );
      };

      const onError = (error: Error) => {
        fail(error);
      };

      const onClose = () => {
        fail(new Error("[Incus.ts] WebSocket connection closed before upgrade completed"));
      };

      socket.on("connect", onConnect);
      socket.on("data", onData);
      socket.on("error", onError);
      socket.on("close", onClose);
    });
  }

  private makeRequestPath(
    path: string,
    query?: Record<string, string | number | boolean | undefined>,
  ): string {
    const url = new URL(normalizePath(path), "http://incus.local");
    if (query) {
      for (const [key, rawValue] of Object.entries(query)) {
        if (rawValue === undefined) {
          continue;
        }

        url.searchParams.set(key, String(rawValue));
      }
    }

    return `${url.pathname}${url.search}`;
  }

  private makeHeaders(options: IncusTransportRequestOptions): Headers {
    const headers = new Headers(this.defaults.headers);
    applyHeaders(headers, options.headers);

    if (this.defaults.userAgent && !headers.has("User-Agent")) {
      headers.set("User-Agent", this.defaults.userAgent);
    }

    if (options.etag) {
      headers.set("If-Match", options.etag);
    }

    if (options.context?.requireAuthenticated) {
      headers.set("X-Incus-authenticated", "true");
    }

    if (this.defaults.oidcTokens?.accessToken && !headers.has("Authorization")) {
      headers.set("Authorization", `Bearer ${this.defaults.oidcTokens.accessToken}`);
    }

    return headers;
  }

  private makeWebsocketHeaders(): Headers {
    const headers = new Headers(this.defaults.headers);

    if (this.defaults.userAgent && !headers.has("User-Agent")) {
      headers.set("User-Agent", this.defaults.userAgent);
    }

    if (this.defaults.oidcTokens?.accessToken && !headers.has("Authorization")) {
      headers.set("Authorization", `Bearer ${this.defaults.oidcTokens.accessToken}`);
    }

    return headers;
  }
}

function normalizeEndpoint(endpoint: string): string {
  if (endpoint.length <= 1) {
    return endpoint;
  }

  return endpoint.endsWith("/") ? endpoint.slice(0, -1) : endpoint;
}

function normalizePath(path: string): string {
  if (path.startsWith("http://") || path.startsWith("https://")) {
    return path;
  }

  return path.startsWith("/") ? path : `/${path}`;
}

function toWsBaseURL(endpoint: string): string {
  const url = new URL(endpoint);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  if (!url.pathname.endsWith("/")) {
    url.pathname = `${url.pathname}/`;
  }

  return url.toString();
}

function applyHeaders(target: Headers, source?: HeadersInit): void {
  if (!source) {
    return;
  }

  const toApply = new Headers(source);
  for (const [key, value] of toApply.entries()) {
    target.set(key, value);
  }
}

function headersToObject(headers: Headers): Record<string, string> {
  const result: Record<string, string> = {};
  for (const [key, value] of headers.entries()) {
    result[key] = value;
  }

  return result;
}

function nodeHeadersToWebHeaders(
  headers: Record<string, string | string[] | undefined>,
): Headers {
  const result = new Headers();
  for (const [key, value] of Object.entries(headers)) {
    if (value === undefined) {
      continue;
    }

    if (Array.isArray(value)) {
      result.set(key, value.join(", "));
    } else {
      result.set(key, value);
    }
  }

  return result;
}

function isIncusEnvelope(value: unknown): value is IncusApiEnvelope {
  if (!value || typeof value !== "object") {
    return false;
  }

  const asRecord = value as Record<string, unknown>;
  return "type" in asRecord && "metadata" in asRecord;
}

async function parseResponseBody(response: Response): Promise<unknown> {
  if (response.status === 204) {
    return null;
  }

  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) {
    return response.json();
  }

  if (
    contentType.startsWith("text/") ||
    contentType.includes("application/openmetrics-text")
  ) {
    return response.text();
  }

  return new Uint8Array(await response.arrayBuffer());
}

function parseRawBody(contentType: string | undefined, bytes: Uint8Array): unknown {
  if (bytes.byteLength === 0) {
    return null;
  }

  const type = contentType ?? "";
  if (type.includes("application/json")) {
    try {
      return JSON.parse(new TextDecoder().decode(bytes));
    } catch {
      return new TextDecoder().decode(bytes);
    }
  }

  if (type.startsWith("text/") || type.includes("application/openmetrics-text")) {
    return new TextDecoder().decode(bytes);
  }

  return bytes;
}

function parseFilters(filters: string[]): string {
  const translated: string[] = [];
  for (const filter of filters) {
    if (!filter.includes("=")) {
      continue;
    }

    const [left, right] = filter.split("=", 2);
    translated.push(`${left} eq ${right}`);
  }

  return translated.join(" and ");
}

function urlsToResourceNames(matchPathPrefix: string, urls: string[]): string[] {
  const normalizedPrefix = `${matchPathPrefix.replace(/\/$/, "")}/`;
  return urls.map((rawUrl) => {
    const parsed = new URL(rawUrl, "http://incus.local");
    const index = parsed.pathname.indexOf(normalizedPrefix);
    if (index < 0) {
      throw new Error(`[Incus.ts] Unexpected resource URL: ${rawUrl}`);
    }

    return decodeURIComponent(parsed.pathname.slice(index + normalizedPrefix.length));
  });
}

function toStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value.filter((entry): entry is string => typeof entry === "string");
}

function toRecord(value: unknown): IncusRecord {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }

  return value as IncusRecord;
}

function toRecordArray(value: unknown): IncusRecord[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .filter((entry): entry is Record<string, unknown> => !!entry && typeof entry === "object")
    .map((entry) => entry as IncusRecord);
}

function flattenOperationMap(value: unknown): IncusRecord[] {
  if (Array.isArray(value)) {
    return toRecordArray(value);
  }

  if (!value || typeof value !== "object") {
    return [];
  }

  const out: IncusRecord[] = [];
  for (const entry of Object.values(value)) {
    if (Array.isArray(entry)) {
      out.push(...toRecordArray(entry));
    }
  }

  return out;
}

function decodeOperationId(operationValue: string): string {
  const cleaned = operationValue.split("?")[0] ?? operationValue;
  const parts = cleaned.split("/").filter(Boolean);
  return decodeURIComponent(parts[parts.length - 1] ?? operationValue);
}

function operationPath(id: string): string {
  return `/1.0/operations/${encodeURIComponent(id)}`;
}

function toReadableStream(data: Uint8Array): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(data);
      controller.close();
    },
  });
}

function toBytes(data: unknown): Uint8Array {
  if (data instanceof Uint8Array) {
    return data;
  }

  if (typeof data === "string") {
    return new TextEncoder().encode(data);
  }

  if (data === null || data === undefined) {
    return new Uint8Array();
  }

  return new TextEncoder().encode(JSON.stringify(data));
}

function toStringRecord(value: unknown): Record<string, string> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }

  const out: Record<string, string> = {};
  for (const [key, entry] of Object.entries(value)) {
    if (typeof entry === "string") {
      out[key] = entry;
    }
  }

  return out;
}

function parseOperationIdFromEnvelope(result: IncusEnvelopeResult<unknown>): string | undefined {
  if (typeof result.operation === "string" && result.operation.length > 0) {
    return decodeOperationId(result.operation);
  }

  if (result.value && typeof result.value === "object") {
    const asRecord = result.value as Record<string, unknown>;
    if (typeof asRecord.id === "string" && asRecord.id.length > 0) {
      return decodeOperationId(asRecord.id);
    }

    if (typeof asRecord.operation === "string" && asRecord.operation.length > 0) {
      return decodeOperationId(asRecord.operation);
    }
  }

  return undefined;
}

function assertOperationSuccess(operation: IncusRecord, source: string): void {
  const err = operation.err;
  if (typeof err === "string" && err.trim().length > 0) {
    throw new Error(`[Incus.ts] Operation ${source} failed: ${err}`);
  }

  const statusCode = operation.status_code;
  if (typeof statusCode === "number" && statusCode >= 400) {
    const status = typeof operation.status === "string" ? ` (${operation.status})` : "";
    throw new Error(
      `[Incus.ts] Operation ${source} failed with status code ${statusCode}${status}`,
    );
  }
}

function parseExecExitCode(operation: IncusRecord): number | undefined {
  const candidates: unknown[] = [
    operation.return,
    operation["return-code"],
    operation.return_code,
    toRecord(operation.metadata).return,
    toRecord(operation.metadata)["return-code"],
    toRecord(operation.metadata).return_code,
  ];

  for (const candidate of candidates) {
    if (typeof candidate === "number" && Number.isFinite(candidate)) {
      return candidate;
    }

    if (typeof candidate === "string" && candidate.length > 0) {
      const parsed = Number.parseInt(candidate, 10);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }

  return undefined;
}

function toExecResult(operation: IncusRecord): IncusExecResult {
  const exitCode = parseExecExitCode(operation);
  return {
    operation,
    exitCode,
    ok: exitCode === undefined ? true : exitCode === 0,
  };
}

function extractExecFds(operationValue: unknown): Record<string, string> {
  const record = toRecord(operationValue);
  const candidates: unknown[] = [
    record.fds,
    toRecord(record.metadata).fds,
    toRecord(toRecord(record.operation).metadata).fds,
    toRecord(toRecord(record.metadata).operation).fds,
  ];

  for (const candidate of candidates) {
    const fds = toStringRecord(candidate);
    if (Object.keys(fds).length > 0) {
      return fds;
    }
  }

  return {};
}

function getBooleanValue(record: IncusRecord, keys: string[]): boolean | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "boolean") {
      return value;
    }
  }

  return undefined;
}

function isInteractiveExecRequest(request: IncusRecord): boolean {
  return getBooleanValue(request, ["interactive", "Interactive"]) ?? false;
}

function hasWaitForWebsocketFlag(request: IncusRecord): boolean {
  return getBooleanValue(request, [
    "wait-for-websocket",
    "wait_for_websocket",
    "waitForWebsocket",
    "waitForWS",
    "WaitForWS",
  ]) !== undefined;
}

function isNodeWebSocket(socket: WebSocketLike): boolean {
  return typeof socket.on === "function";
}

function addWebSocketMessageListener(
  socket: WebSocketLike,
  listener: (data: unknown) => void,
): () => void {
  if (isNodeWebSocket(socket) && socket.on && socket.off) {
    const nodeListener = (data: unknown) => {
      listener(data);
    };
    socket.on("message", nodeListener);
    return () => {
      socket.off?.("message", nodeListener);
    };
  }

  const domListener = (event: unknown) => {
    const data = event && typeof event === "object" && "data" in event
      ? (event as { data: unknown }).data
      : event;
    listener(data);
  };

  socket.addEventListener?.("message", domListener);
  return () => {
    socket.removeEventListener?.("message", domListener);
  };
}

function addWebSocketCloseListener(socket: WebSocketLike, listener: () => void): () => void {
  if (isNodeWebSocket(socket) && socket.on && socket.off) {
    const nodeListener = () => {
      listener();
    };
    socket.on("close", nodeListener);
    return () => {
      socket.off?.("close", nodeListener);
    };
  }

  const domListener = () => {
    listener();
  };
  socket.addEventListener?.("close", domListener);
  return () => {
    socket.removeEventListener?.("close", domListener);
  };
}

function addWebSocketErrorListener(
  socket: WebSocketLike,
  listener: (error: unknown) => void,
): () => void {
  if (isNodeWebSocket(socket) && socket.on && socket.off) {
    const nodeListener = (error: unknown) => {
      listener(error);
    };
    socket.on("error", nodeListener);
    return () => {
      socket.off?.("error", nodeListener);
    };
  }

  const domListener = (event: unknown) => {
    listener(event);
  };
  socket.addEventListener?.("error", domListener);
  return () => {
    socket.removeEventListener?.("error", domListener);
  };
}

function closeWebSocketSafely(socket: WebSocketLike): void {
  try {
    socket.close();
  } catch {
    // Best effort.
  }
}

type AsyncQueue<T> = {
  iterable: AsyncIterable<T>;
  push(value: T): void;
  close(): void;
  fail(error: unknown): void;
  done: Promise<void>;
};

function createAsyncQueue<T>(): AsyncQueue<T> {
  const items: T[] = [];
  const waiters: Array<{
    resolve: (result: IteratorResult<T>) => void;
    reject: (reason?: unknown) => void;
  }> = [];
  let closed = false;
  let failure: unknown | undefined;
  let resolveDone!: () => void;
  const done = new Promise<void>((resolve) => {
    resolveDone = resolve;
  });

  const settleDone = () => {
    resolveDone();
  };

  const close = () => {
    if (closed) {
      return;
    }

    closed = true;
    while (waiters.length > 0) {
      const waiter = waiters.shift();
      waiter?.resolve({ done: true, value: undefined as T });
    }
    settleDone();
  };

  const fail = (error: unknown) => {
    if (closed) {
      return;
    }

    failure = error;
    closed = true;
    while (waiters.length > 0) {
      const waiter = waiters.shift();
      waiter?.reject(error);
    }
    settleDone();
  };

  const push = (value: T) => {
    if (closed) {
      return;
    }

    const waiter = waiters.shift();
    if (waiter) {
      waiter.resolve({ done: false, value });
      return;
    }

    items.push(value);
  };

  const iterable: AsyncIterable<T> = {
    [Symbol.asyncIterator]() {
      return {
        next: () => {
          if (items.length > 0) {
            const value = items.shift();
            return Promise.resolve({ done: false, value: value as T });
          }

          if (failure !== undefined) {
            return Promise.reject(failure);
          }

          if (closed) {
            return Promise.resolve({ done: true, value: undefined as T });
          }

          return new Promise<IteratorResult<T>>((resolve, reject) => {
            waiters.push({ resolve, reject });
          });
        },
      };
    },
  };

  return { iterable, push, close, fail, done };
}

function createAwaitableOperation(
  operationReadyInput: PromiseLike<IncusOperation> | IncusOperation,
  defaultWaitOptions: IncusOperationWaitOptions = {},
): IncusAwaitableOperation {
  const operationReady = Promise.resolve(operationReadyInput);
  let operationId = "";
  void operationReady.then((operation) => {
    operationId = operation.id;
  }).catch(() => {
    // Best effort.
  });

  const wait = (options: IncusOperationWaitOptions = defaultWaitOptions) => (
    operationReady.then((operation) => operation.wait(options))
  );

  let defaultWaitPromise: Promise<IncusRecord> | undefined;
  const getDefaultWaitPromise = () => {
    defaultWaitPromise ??= wait(defaultWaitOptions);
    return defaultWaitPromise;
  };

  return {
    get id(): string {
      return operationId;
    },
    wait,
    cancel: () => operationReady.then((operation) => operation.cancel()),
    refresh: () => operationReady.then((operation) => operation.refresh()),
    websocket: (secret: string) => operationReady.then((operation) => operation.websocket(secret)),
    onUpdate: (handler: (operation: IncusRecord) => void) => (
      operationReady.then((operation) => operation.onUpdate(handler))
    ),
    then: <TResult1 = IncusRecord, TResult2 = never>(
      onFulfilled?: ((value: IncusRecord) => TResult1 | PromiseLike<TResult1>) | null,
      onRejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
    ): Promise<TResult1 | TResult2> => (
      getDefaultWaitPromise().then(onFulfilled, onRejected)
    ),
    catch: <TResult = never>(
      onRejected?: ((reason: unknown) => TResult | PromiseLike<TResult>) | null,
    ): Promise<IncusRecord | TResult> => (
      getDefaultWaitPromise().catch(onRejected)
    ),
    finally: (onFinally?: (() => void) | null): Promise<IncusRecord> => (
      getDefaultWaitPromise().finally(onFinally ?? undefined)
    ),
  };
}

async function webSocketDataToBytes(data: unknown): Promise<Uint8Array> {
  if (data instanceof Uint8Array) {
    return data;
  }

  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  }

  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  }

  if (data instanceof Blob) {
    return new Uint8Array(await data.arrayBuffer());
  }

  if (typeof data === "string") {
    return new TextEncoder().encode(data);
  }

  return toBytes(data);
}

function streamWebSocketToWritable(
  socket: WebSocketLike,
  options: {
    output?: WritableStream<Uint8Array>;
    onChunk?: (chunk: Uint8Array) => void;
    onError?: (error: unknown) => void;
    onClose?: () => void;
  } = {},
): Promise<void> {
  const writer = options.output?.getWriter();
  let writeChain = Promise.resolve();
  let finished = false;
  const detachments: Array<() => void> = [];
  let resolveDone!: () => void;
  const done = new Promise<void>((resolve) => {
    resolveDone = resolve;
  });

  const finish = (error?: unknown) => {
    if (finished) {
      return;
    }

    finished = true;
    for (const detach of detachments) {
      detach();
    }

    void writeChain.finally(async () => {
      if (error !== undefined) {
        options.onError?.(error);
      } else {
        options.onClose?.();
      }

      if (writer) {
        try {
          await writer.close();
        } catch {
          // Best effort.
        }

        writer.releaseLock();
      }

      resolveDone();
    });
  };

  detachments.push(
    addWebSocketMessageListener(socket, (data) => {
      writeChain = writeChain.then(async () => {
        const bytes = await webSocketDataToBytes(data);
        options.onChunk?.(bytes);
        if (writer) {
          await writer.write(bytes);
        }
      }).catch((error) => {
        finish(error);
      });
    }),
  );
  detachments.push(addWebSocketCloseListener(socket, () => finish()));
  detachments.push(addWebSocketErrorListener(socket, (error) => finish(error)));

  return done;
}

async function sendToWebSocket(socket: WebSocketLike, data: Uint8Array): Promise<void> {
  if (isNodeWebSocket(socket)) {
    const send = (socket as unknown as {
      send: (
        payload: Uint8Array,
        callback?: (error?: Error) => void,
      ) => void;
    }).send;

    // Unix-socket websocket shims expose a synchronous `send(data)` API, while
    // libraries like `ws` use the callback form to signal flush completion.
    if (send.length < 2) {
      send.call(socket, data);
      return;
    }

    await new Promise<void>((resolve, reject) => {
      try {
        send.call(socket, data, (error?: Error) => {
          if (error) {
            reject(error);
            return;
          }

          resolve();
        });
      } catch (error) {
        reject(error);
      }
    });
    return;
  }

  socket.send(data);
}

async function* toInputChunks(input: IncusBinaryInput): AsyncGenerator<Uint8Array> {
  if (input instanceof Uint8Array) {
    yield input;
    return;
  }

  if (input instanceof ArrayBuffer) {
    yield new Uint8Array(input);
    return;
  }

  if (ArrayBuffer.isView(input)) {
    yield new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
    return;
  }

  if (input instanceof Blob) {
    yield new Uint8Array(await input.arrayBuffer());
    return;
  }

  const reader = input.getReader();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }

      if (value) {
        yield value;
      }
    }
  } finally {
    reader.releaseLock();
  }
}

async function writeInputToWebSocket(socket: WebSocketLike, input?: IncusBinaryInput): Promise<void> {
  if (!input) {
    closeWebSocketSafely(socket);
    return;
  }

  try {
    for await (const chunk of toInputChunks(input)) {
      await sendToWebSocket(socket, chunk);
    }
  } finally {
    closeWebSocketSafely(socket);
  }
}

function parseFileHeaders(headers: Headers | undefined): FileHeaders {
  if (!headers) {
    return {};
  }

  const uidRaw = headers.get("x-incus-uid");
  const gidRaw = headers.get("x-incus-gid");
  const modeRaw = headers.get("x-incus-mode");
  const typeRaw = headers.get("x-incus-type");

  return {
    uid: uidRaw ? Number.parseInt(uidRaw, 10) : undefined,
    gid: gidRaw ? Number.parseInt(gidRaw, 10) : undefined,
    mode: modeRaw ? Number.parseInt(modeRaw, 8) : undefined,
    type: typeRaw ?? undefined,
  };
}

function createNotImplementedProxy(path: string[]): unknown {
  const fn = (..._args: unknown[]) =>
    Promise.reject(
      new Error(`[Incus.ts] ${path.join(".")}() is not implemented yet`),
    );

  return new Proxy(fn, {
    get(_target, property) {
      if (property === "then") {
        return undefined;
      }

      if (typeof property === "symbol") {
        return undefined;
      }

      return createNotImplementedProxy([...path, String(property)]);
    },
    apply() {
      return Promise.reject(
        new Error(`[Incus.ts] ${path.join(".")}() is not implemented yet`),
      );
    },
  });
}

function createApiWithFallback<T extends object>(root: string, implemented: Partial<T>): T {
  return new Proxy(implemented as T, {
    get(target, property, receiver) {
      if (typeof property === "symbol") {
        return Reflect.get(target, property, receiver);
      }

      if (Reflect.has(target, property)) {
        return Reflect.get(target, property, receiver);
      }

      return createNotImplementedProxy([root, String(property)]);
    },
  });
}

function createRemoteOperationFromTarget(
  targetOperation: IncusOperation | null,
): IncusRemoteOperation {
  return {
    wait: async () => {
      if (targetOperation) {
        await targetOperation.wait();
      }
    },
    cancelTarget: async () => {
      if (targetOperation) {
        await targetOperation.cancel();
      }
    },
    target: async () => {
      if (!targetOperation) {
        return null;
      }

      return targetOperation.refresh();
    },
    onUpdate: async (handler) => {
      if (!targetOperation) {
        return async () => {};
      }

      return targetOperation.onUpdate(handler);
    },
  };
}

function prepareBody(body: unknown): {
  body: BodyInit | null | undefined;
  contentType?: string;
} {
  if (body === undefined || body === null) {
    return { body: undefined };
  }

  if (
    body instanceof Blob ||
    body instanceof ArrayBuffer ||
    ArrayBuffer.isView(body) ||
    body instanceof ReadableStream ||
    typeof body === "string" ||
    body instanceof URLSearchParams ||
    body instanceof FormData
  ) {
    return { body: body as BodyInit };
  }

  return {
    body: JSON.stringify(body),
    contentType: "application/json",
  };
}

async function prepareUnixBody(body: unknown): Promise<{
  bodyBytes?: Uint8Array;
  contentType?: string;
}> {
  if (body === undefined || body === null) {
    return {};
  }

  if (typeof body === "string") {
    return { bodyBytes: new TextEncoder().encode(body), contentType: "text/plain" };
  }

  if (body instanceof Uint8Array) {
    return { bodyBytes: body };
  }

  if (body instanceof ArrayBuffer) {
    return { bodyBytes: new Uint8Array(body) };
  }

  if (ArrayBuffer.isView(body)) {
    return { bodyBytes: new Uint8Array(body.buffer, body.byteOffset, body.byteLength) };
  }

  if (body instanceof Blob) {
    return { bodyBytes: new Uint8Array(await body.arrayBuffer()) };
  }

  if (body instanceof ReadableStream) {
    throw new Error("[Incus.ts] ReadableStream body over Unix socket is not implemented yet");
  }

  return {
    bodyBytes: new TextEncoder().encode(JSON.stringify(body)),
    contentType: "application/json",
  };
}

export class IncusImageClient {
  readonly connection: ConnectionApi;
  readonly images: ImagesApi;
  readonly raw: RawApi;

  protected readonly contextState: Readonly<IncusRequestContext>;
  protected readonly kind: IncusConnectKind;
  private serverSnapshot?: IncusRecord;

  constructor(
    protected readonly transport: IncusTransport,
    readonly endpoint: string,
    readonly options: Readonly<IncusConnectionOptions> = {},
    context: Readonly<IncusRequestContext> = {},
    kind: IncusConnectKind = "incus",
  ) {
    this.contextState = context;
    this.kind = kind;
    this.connection = this.createConnectionApi();
    this.raw = this.createRawApi();
    this.images = this.createImagesApi();
  }

  get context(): Readonly<IncusRequestContext> {
    return this.contextState;
  }

  disconnect(): void {
    this.transport.close?.();
  }

  protected async requestTransport(
    method: string,
    path: string,
    options: InternalRequestOptions = {},
  ): Promise<IncusTransportResponse<unknown>> {
    const { applyContext = true, ...rest } = options;
    return this.transport.request(method, path, {
      ...rest,
      context: applyContext ? this.contextState : undefined,
    });
  }

  protected async requestEnvelope<T = unknown>(
    method: string,
    path: string,
    options: InternalRequestOptions = {},
  ): Promise<IncusEnvelopeResult<T>> {
    const response = await this.requestTransport(method, path, options);
    const payload = response.data;

    if (isIncusEnvelope(payload)) {
      if (payload.type === "error" || response.status >= 400) {
        throw new IncusApiError(
          payload.error ?? `Incus request failed (${response.status})`,
          response.status,
          {
            statusCode: payload.status_code,
            errorCode: payload.error_code,
            details: payload.metadata,
          },
        );
      }

      return {
        value: payload.metadata as T,
        etag: response.etag,
        operation: payload.operation,
        status: response.status,
        headers: response.headers,
      };
    }

    if (response.status >= 400) {
      throw new IncusApiError(
        typeof payload === "string"
          ? payload
          : `Incus request failed (${response.status})`,
        response.status,
        { details: payload },
      );
    }

    return {
      value: payload as T,
      etag: response.etag,
      status: response.status,
      headers: response.headers,
    };
  }

  protected async requestBinary(
    method: string,
    path: string,
    options: InternalRequestOptions = {},
  ): Promise<IncusEnvelopeResult<Uint8Array>> {
    const response = await this.requestTransport(method, path, options);
    const payload = response.data;

    if (isIncusEnvelope(payload)) {
      if (payload.type === "error" || response.status >= 400) {
        throw new IncusApiError(
          payload.error ?? `Incus request failed (${response.status})`,
          response.status,
          {
            statusCode: payload.status_code,
            errorCode: payload.error_code,
            details: payload.metadata,
          },
        );
      }

      return {
        value: toBytes(payload.metadata),
        etag: response.etag,
        operation: payload.operation,
        status: response.status,
        headers: response.headers,
      };
    }

    if (response.status >= 400) {
      throw new IncusApiError(
        typeof payload === "string"
          ? payload
          : `Incus request failed (${response.status})`,
        response.status,
        { details: payload },
      );
    }

    return {
      value: toBytes(payload),
      etag: response.etag,
      status: response.status,
      headers: response.headers,
    };
  }

  protected createOperationFromRequest(
    method: string,
    path: string,
    options: InternalRequestOptions = {},
  ): IncusAwaitableOperation {
    const operationReady = this.requestEnvelope(method, path, options)
      .then((result) => this.createOperationFromEnvelopeResult(method, path, result));
    return createAwaitableOperation(operationReady);
  }

  protected createOperationFromEnvelopeResult(
    method: string,
    path: string,
    result: IncusEnvelopeResult<unknown>,
  ): IncusOperation {
    const operationId = parseOperationIdFromEnvelope(result);
    if (!operationId) {
      throw new Error(
        `[Incus.ts] Missing operation id in response for ${method.toUpperCase()} ${path}`,
      );
    }

    return this.createOperation(operationId);
  }

  protected createOperation(id: string): IncusOperation {
    return {
      id,
      wait: async (options = {}) => {
        const result = await this.requestEnvelope<IncusRecord>(
          "GET",
          `${operationPath(id)}/wait`,
          {
            query: {
              timeout: options.timeoutSeconds ?? -1,
            },
            signal: options.signal,
          },
        );
        const operation = toRecord(result.value);
        assertOperationSuccess(operation, id);
        return operation;
      },
      cancel: async () => {
        await this.requestEnvelope("DELETE", operationPath(id));
      },
      refresh: async () => {
        const result = await this.requestEnvelope<IncusRecord>(
          "GET",
          operationPath(id),
        );
        return toRecord(result.value);
      },
      websocket: async (secret: string) => {
        return this.openWebsocket(`${operationPath(id)}/websocket?secret=${encodeURIComponent(secret)}`);
      },
      onUpdate: async (handler) => {
        const timer = setInterval(async () => {
          try {
            const operation = await this.requestEnvelope<IncusRecord>(
              "GET",
              operationPath(id),
            );
            handler(toRecord(operation.value));
          } catch {
            // Best effort.
          }
        }, 1000);

        return async () => {
          clearInterval(timer);
        };
      },
    };
  }

  protected async openWebsocket(
    path: string,
    options: { applyContext?: boolean } = {},
  ): Promise<WebSocket> {
    if (!this.transport.websocket) {
      throw new Error("[Incus.ts] Transport does not support websocket connections");
    }

    let finalPath = path;
    if (options.applyContext !== false) {
      const url = new URL(normalizePath(path), "http://incus.local");
      if (this.context.target && !url.searchParams.has("target")) {
        url.searchParams.set("target", this.context.target);
      }

      if (
        this.context.project &&
        !url.searchParams.has("project") &&
        !url.searchParams.has("all-projects")
      ) {
        url.searchParams.set("project", this.context.project);
      }

      finalPath = `${url.pathname}${url.search}`;
    }

    return this.transport.websocket(finalPath);
  }

  private async getServerSnapshot(): Promise<IncusRecord | undefined> {
    if (this.serverSnapshot) {
      return this.serverSnapshot;
    }

    if (
      this.kind === "simple-streams" ||
      this.kind === "incus-public"
    ) {
      return undefined;
    }

    try {
      const server = await this.requestEnvelope<IncusRecord>("GET", "/1.0");
      this.serverSnapshot = toRecord(server.value);
      return this.serverSnapshot;
    } catch {
      return undefined;
    }
  }

  private createConnectionApi(): ConnectionApi {
    return createApiWithFallback<ConnectionApi>("connection", {
      info: async () => {
        const server = await this.getServerSnapshot();
        const env = toRecord(server?.environment);
        const addresses = Array.isArray(env.addresses)
          ? env.addresses.filter((entry): entry is string => typeof entry === "string")
          : [];
        const httpsAddresses = addresses
          .filter((entry) => !entry.startsWith(":"))
          .map((entry) => `https://${entry}`);

        const socketPath = this.endpoint.startsWith("unix://")
          ? this.endpoint.replace(/^unix:\/\//, "")
          : undefined;

        return {
          addresses: httpsAddresses,
          certificate: typeof env.certificate === "string" ? env.certificate : undefined,
          protocol: this.kind === "simple-streams" ? "simplestreams" : "incus",
          url: this.endpoint.startsWith("unix://") ? undefined : this.endpoint,
          socketPath,
          project: this.context.project ?? "default",
          target: this.context.target
            ?? (typeof env.server_name === "string" ? env.server_name : undefined),
        };
      },
      httpClient: async () => this.options.fetch ?? fetch,
      doHttp: async (request: Request) => {
        const fetchImpl = this.options.fetch ?? fetch;
        return fetchImpl(request);
      },
      disconnect: () => this.disconnect(),
    });
  }

  private createRawApi(): RawApi {
    return createApiWithFallback<RawApi>("raw", {
      query: async <T = IncusRecord>(
        method: string,
        path: string,
        body?: unknown,
        options: IncusMutationOptions = {},
      ) => {
        const result = await this.requestEnvelope<T>(method, path, {
          body,
          etag: options.etag,
          applyContext: false,
        });
        return { value: result.value, etag: result.etag };
      },
      websocket: async (path: string) => this.openWebsocket(path, { applyContext: false }),
      operation: (
        method: string,
        path: string,
        body?: unknown,
        options: IncusMutationOptions = {},
      ) => {
        return this.createOperationFromRequest(method, path, {
          body,
          etag: options.etag,
          applyContext: false,
        });
      },
    });
  }

  private createImageAliasesApi(): ImageAliasesApi {
    return createApiWithFallback<ImageAliasesApi>("images.aliases", {
      list: async () => {
        const result = await this.requestEnvelope<IncusRecord[]>(
          "GET",
          "/1.0/images/aliases",
          { query: { recursion: 1 } },
        );
        return toRecordArray(result.value);
      },
      names: async () => {
        const result = await this.requestEnvelope<unknown[]>(
          "GET",
          "/1.0/images/aliases",
        );
        return urlsToResourceNames("/images/aliases", toStringArray(result.value));
      },
      get: async (name: string) => {
        const result = await this.requestEnvelope<IncusRecord>(
          "GET",
          `/1.0/images/aliases/${encodeURIComponent(name)}`,
        );
        return { value: toRecord(result.value), etag: result.etag };
      },
      getArchitectures: async (name: string) => {
        const alias = await this.requestEnvelope<IncusRecord>(
          "GET",
          `/1.0/images/aliases/${encodeURIComponent(name)}`,
        );
        const target = toRecord(alias.value).target;
        if (typeof target !== "string") {
          return {};
        }

        const image = await this.requestEnvelope<IncusRecord>(
          "GET",
          `/1.0/images/${encodeURIComponent(target)}`,
        );
        const architecture = toRecord(image.value).architecture;
        if (typeof architecture !== "string") {
          return {};
        }

        return { [architecture]: toRecord(alias.value) };
      },
      create: async (alias: IncusRecord) => {
        await this.requestEnvelope("POST", "/1.0/images/aliases", { body: alias });
      },
      update: async (
        name: string,
        alias: IncusRecord,
        options: IncusMutationOptions = {},
      ) => {
        await this.requestEnvelope(
          "PUT",
          `/1.0/images/aliases/${encodeURIComponent(name)}`,
          { body: alias, etag: options.etag },
        );
      },
      rename: async (name: string, alias: IncusRecord) => {
        await this.requestEnvelope(
          "POST",
          `/1.0/images/aliases/${encodeURIComponent(name)}`,
          { body: alias },
        );
      },
      remove: async (name: string) => {
        await this.requestEnvelope(
          "DELETE",
          `/1.0/images/aliases/${encodeURIComponent(name)}`,
        );
      },
    });
  }

  private createImagesApi(): ImagesApi {
    if (this.kind === "simple-streams") {
      return createApiWithFallback<ImagesApi>("images", {
        aliases: createNotImplementedProxy(["images", "aliases"]) as ImageAliasesApi,
      });
    }

    return createApiWithFallback<ImagesApi>("images", {
      list: async (options = {}) => {
        const query: Record<string, string | number | boolean | undefined> = {
          recursion: 1,
        };
        if (options.allProjects) {
          query["all-projects"] = true;
        }

        if (options.filter && options.filter.length > 0) {
          query.filter = parseFilters(options.filter);
        }

        const result = await this.requestEnvelope<IncusRecord[]>("GET", "/1.0/images", {
          query,
        });
        return toRecordArray(result.value);
      },
      fingerprints: async (options = {}) => {
        if (options.filter?.length || options.allProjects) {
          const images = await this.images.list(options);
          return images
            .map((image) => image.fingerprint)
            .filter((entry): entry is string => typeof entry === "string");
        }

        const result = await this.requestEnvelope<unknown[]>("GET", "/1.0/images");
        return urlsToResourceNames("/images", toStringArray(result.value));
      },
      get: async (
        fingerprint: string,
        options: { secret?: string } = {},
      ) => {
        const result = await this.requestEnvelope<IncusRecord>(
          "GET",
          `/1.0/images/${encodeURIComponent(fingerprint)}`,
          {
            query: options.secret ? { secret: options.secret } : undefined,
          },
        );
        return { value: toRecord(result.value), etag: result.etag };
      },
      create: (image: IncusRecord, upload?: IncusRecord) => {
        const body = upload ? { ...image, upload } : image;
        return this.createOperationFromRequest("POST", "/1.0/images", { body });
      },
      copyFrom: async (
        _source: IncusImageClient,
        _image: IncusRecord,
        _options?: IncusRecord,
      ) => createRemoteOperationFromTarget(null),
      update: async (
        fingerprint: string,
        image: IncusRecord,
        options: IncusMutationOptions = {},
      ) => {
        await this.requestEnvelope(
          "PUT",
          `/1.0/images/${encodeURIComponent(fingerprint)}`,
          {
            body: image,
            etag: options.etag,
          },
        );
      },
      remove: (fingerprint: string) => {
        return this.createOperationFromRequest(
          "DELETE",
          `/1.0/images/${encodeURIComponent(fingerprint)}`,
        );
      },
      refresh: (fingerprint: string) => {
        return this.createOperationFromRequest(
          "POST",
          `/1.0/images/${encodeURIComponent(fingerprint)}/refresh`,
        );
      },
      createSecret: (fingerprint: string) => {
        return this.createOperationFromRequest(
          "POST",
          `/1.0/images/${encodeURIComponent(fingerprint)}/secret`,
        );
      },
      export: (fingerprint: string, request?: IncusRecord) => {
        return this.createOperationFromRequest(
          "POST",
          `/1.0/images/${encodeURIComponent(fingerprint)}/export`,
          { body: request ?? {} },
        );
      },
      aliases: this.createImageAliasesApi(),
    });
  }
}

export class IncusClient extends IncusImageClient {
  readonly server: ServerApi;
  readonly certificates: CertificatesApi;
  readonly instances: InstancesApi;
  readonly events: EventsApi;
  readonly metadata: MetadataApi;
  readonly networks: NetworksApi;
  readonly operations: OperationsApi;
  readonly profiles: ProfilesApi;
  readonly projects: ProjectsApi;
  readonly storage: StorageApi;
  readonly cluster: ClusterApi;
  readonly warnings: WarningsApi;
  readonly oidc: OidcApi;

  constructor(
    transport: IncusTransport,
    endpoint: string,
    options: Readonly<IncusConnectionOptions> = {},
    context: Readonly<IncusRequestContext> = {},
    kind: IncusConnectKind = "incus",
  ) {
    super(transport, endpoint, options, context, kind);

    this.server = this.createServerApi();
    this.certificates = createNotImplementedProxy(["certificates"]) as CertificatesApi;
    this.instances = this.createInstancesApi();
    this.events = createNotImplementedProxy(["events"]) as EventsApi;
    this.metadata = createApiWithFallback<MetadataApi>("metadata", {
      configuration: async () => {
        const result = await this.requestEnvelope<IncusRecord>(
          "GET",
          "/1.0/metadata/configuration",
        );
        return toRecord(result.value);
      },
    });
    this.networks = createNotImplementedProxy(["networks"]) as NetworksApi;
    this.operations = this.createOperationsApi();
    this.profiles = createNotImplementedProxy(["profiles"]) as ProfilesApi;
    this.projects = createNotImplementedProxy(["projects"]) as ProjectsApi;
    this.storage = createNotImplementedProxy(["storage"]) as StorageApi;
    this.cluster = createNotImplementedProxy(["cluster"]) as ClusterApi;
    this.warnings = createNotImplementedProxy(["warnings"]) as WarningsApi;
    this.oidc = createApiWithFallback<OidcApi>("oidc", {
      tokens: async () => {
        if (!this.options.oidcTokens) {
          return null;
        }

        return { ...this.options.oidcTokens };
      },
    });
  }

  withContext(contextPatch: Partial<IncusRequestContext>): IncusClient {
    return new IncusClient(this.transport, this.endpoint, this.options, {
      ...this.context,
      ...contextPatch,
    }, this.kind);
  }

  project(name: string): IncusClient {
    return this.withContext({ project: name });
  }

  target(name: string): IncusClient {
    return this.withContext({ target: name });
  }

  requireAuthenticated(authenticated = true): IncusClient {
    return this.withContext({ requireAuthenticated: authenticated });
  }

  private createServerApi(): ServerApi {
    return createApiWithFallback<ServerApi>("server", {
      metrics: async () => {
        const result = await this.requestEnvelope<string>("GET", "/1.0/metrics");
        return typeof result.value === "string"
          ? result.value
          : new TextDecoder().decode(toBytes(result.value));
      },
      get: async () => {
        const result = await this.requestEnvelope<IncusRecord>("GET", "/1.0");
        return { value: toRecord(result.value), etag: result.etag };
      },
      resources: async () => {
        const result = await this.requestEnvelope<IncusRecord>("GET", "/1.0/resources");
        return toRecord(result.value);
      },
      update: async (
        server: IncusRecord,
        options: IncusMutationOptions = {},
      ) => {
        await this.requestEnvelope("PUT", "/1.0", {
          body: server,
          etag: options.etag,
        });
      },
      applyPreseed: async (config: IncusRecord) => {
        await this.requestEnvelope("PUT", "/1.0", { body: config });
      },
      hasExtension: async (extension: string) => {
        const result = await this.requestEnvelope<IncusRecord>("GET", "/1.0");
        const server = toRecord(result.value);
        const apiExtensions = server.api_extensions;
        if (!Array.isArray(apiExtensions)) {
          return false;
        }

        return apiExtensions.some((entry) => entry === extension);
      },
      isClustered: async () => {
        const result = await this.requestEnvelope<IncusRecord>("GET", "/1.0");
        const env = toRecord(toRecord(result.value).environment);
        return env.server_clustered === true;
      },
    });
  }

  private createOperationsApi(): OperationsApi {
    return createApiWithFallback<OperationsApi>("operations", {
      uuids: async () => {
        const result = await this.requestEnvelope<unknown[]>("GET", "/1.0/operations");
        return urlsToResourceNames("/operations", toStringArray(result.value));
      },
      list: async (options = {}) => {
        const result = await this.requestEnvelope<unknown>("GET", "/1.0/operations", {
          query: {
            recursion: 1,
            "all-projects": options.allProjects ? "true" : undefined,
          },
        });
        return flattenOperationMap(result.value);
      },
      get: async (uuid: string) => {
        const result = await this.requestEnvelope<IncusRecord>(
          "GET",
          `/1.0/operations/${encodeURIComponent(uuid)}`,
        );
        return { value: toRecord(result.value), etag: result.etag };
      },
      wait: async (uuid: string, timeoutSeconds = -1) => {
        const result = await this.requestEnvelope<IncusRecord>(
          "GET",
          `/1.0/operations/${encodeURIComponent(uuid)}/wait`,
          {
            query: { timeout: timeoutSeconds },
          },
        );
        const operation = toRecord(result.value);
        assertOperationSuccess(operation, uuid);
        return { value: operation, etag: result.etag };
      },
      waitWithSecret: async (
        uuid: string,
        secret: string,
        timeoutSeconds = -1,
      ) => {
        const result = await this.requestEnvelope<IncusRecord>(
          "GET",
          `/1.0/operations/${encodeURIComponent(uuid)}/wait`,
          {
            query: {
              secret,
              timeout: timeoutSeconds,
            },
          },
        );
        const operation = toRecord(result.value);
        assertOperationSuccess(operation, uuid);
        return { value: operation, etag: result.etag };
      },
      websocket: async (uuid: string, secret: string) => {
        const query = secret ? `?secret=${encodeURIComponent(secret)}` : "";
        return this.openWebsocket(
          `/1.0/operations/${encodeURIComponent(uuid)}/websocket${query}`,
        );
      },
      remove: async (uuid: string) => {
        await this.requestEnvelope(
          "DELETE",
          `/1.0/operations/${encodeURIComponent(uuid)}`,
        );
      },
    });
  }

  private isPipeTarget(target: InstanceExecOptions["stdout"]): target is "pipe" {
    return target === "pipe";
  }

  private toWritableTarget(
    target: InstanceExecOptions["stdout"],
  ): WritableStream<Uint8Array> | undefined {
    return this.isPipeTarget(target) ? undefined : target;
  }

  private shouldAttachExecIO(options?: InstanceExecOptions): boolean {
    return Boolean(
      options?.stdin ||
      options?.stdout ||
      options?.stderr ||
      options?.onControl,
    );
  }

  private shouldPipeExecIO(options?: InstanceExecOptions): boolean {
    return Boolean(
      this.isPipeTarget(options?.stdout) ||
      this.isPipeTarget(options?.stderr),
    );
  }

  private async getExecFds(
    operation: IncusOperation,
    initialOperationValue: unknown,
  ): Promise<Record<string, string>> {
    const initial = extractExecFds(initialOperationValue);
    if (Object.keys(initial).length > 0) {
      return initial;
    }

    const refreshed = await operation.refresh();
    return extractExecFds(refreshed);
  }

  private async attachExecWebsockets(
    operation: IncusOperation,
    request: IncusRecord,
    options?: InstanceExecOptions,
    initialOperationValue?: unknown,
    hooks: {
      onStdoutChunk?: (chunk: Uint8Array) => void;
      onStderrChunk?: (chunk: Uint8Array) => void;
      onStdoutError?: (error: unknown) => void;
      onStderrError?: (error: unknown) => void;
      onStdoutClose?: () => void;
      onStderrClose?: () => void;
    } = {},
  ): Promise<{ hasStdout: boolean; hasStderr: boolean }> {
    const fds = await this.getExecFds(operation, initialOperationValue ?? {});
    if (Object.keys(fds).length === 0) {
      return { hasStdout: false, hasStderr: false };
    }

    const stdoutWritable = this.toWritableTarget(options?.stdout);
    const stderrWritable = this.toWritableTarget(options?.stderr);
    let hasStdout = false;
    let hasStderr = false;

    const openChannel = async (name: string): Promise<WebSocketLike | null> => {
      const secret = fds[name];
      if (!secret) {
        return null;
      }

      const socket = (await operation.websocket(secret)) as unknown as WebSocketLike;
      if (options?.signal) {
        options.signal.addEventListener(
          "abort",
          () => {
            closeWebSocketSafely(socket);
          },
          { once: true },
        );
      }

      return socket;
    };

    const controlSocket = await openChannel("control");
    if (controlSocket) {
      void streamWebSocketToWritable(controlSocket);
      options?.onControl?.(controlSocket as unknown as WebSocket);
    }

    if (isInteractiveExecRequest(request)) {
      const interactiveSocket = await openChannel("0");
      if (!interactiveSocket) {
        return { hasStdout: false, hasStderr: false };
      }

      hasStdout = true;
      void streamWebSocketToWritable(interactiveSocket, {
        output: stdoutWritable,
        onChunk: hooks.onStdoutChunk,
        onError: hooks.onStdoutError,
        onClose: hooks.onStdoutClose,
      });
      void writeInputToWebSocket(interactiveSocket, options?.stdin);
      return { hasStdout, hasStderr };
    }

    const stdinSocket = await openChannel("0");
    if (stdinSocket) {
      void writeInputToWebSocket(stdinSocket, options?.stdin);
    }

    const stdoutSocket = await openChannel("1");
    if (stdoutSocket) {
      hasStdout = true;
      void streamWebSocketToWritable(stdoutSocket, {
        output: stdoutWritable,
        onChunk: hooks.onStdoutChunk,
        onError: hooks.onStdoutError,
        onClose: hooks.onStdoutClose,
      });
    }

    const stderrSocket = await openChannel("2");
    if (stderrSocket) {
      hasStderr = true;
      void streamWebSocketToWritable(stderrSocket, {
        output: stderrWritable,
        onChunk: hooks.onStderrChunk,
        onError: hooks.onStderrError,
        onClose: hooks.onStderrClose,
      });
    }

    return { hasStdout, hasStderr };
  }

  private createInstancesApi(): InstancesApi {
    const toInstancePath = (instanceName: string) =>
      `/1.0/instances/${encodeURIComponent(instanceName)}`;
    const toSnapshotPath = (instanceName: string, snapshotName?: string) => (
      snapshotName
        ? `${toInstancePath(instanceName)}/snapshots/${encodeURIComponent(snapshotName)}`
        : `${toInstancePath(instanceName)}/snapshots`
    );

    const createLogsApi = (instanceName: string): InstanceLogsApi => ({
      list: async () => {
        const result = await this.requestEnvelope<unknown[]>(
          "GET",
          `${toInstancePath(instanceName)}/logs`,
        );
        return urlsToResourceNames(
          `/instances/${encodeURIComponent(instanceName)}/logs`,
          toStringArray(result.value),
        );
      },
      get: async (filename: string) => {
        const result = await this.requestBinary(
          "GET",
          `${toInstancePath(instanceName)}/logs/${encodeURIComponent(filename)}`,
        );
        return toReadableStream(result.value);
      },
      remove: async (filename: string) => {
        await this.requestEnvelope(
          "DELETE",
          `${toInstancePath(instanceName)}/logs/${encodeURIComponent(filename)}`,
        );
      },
      getConsole: async () => {
        const result = await this.requestBinary(
          "GET",
          `${toInstancePath(instanceName)}/console`,
        );
        return toReadableStream(result.value);
      },
      removeConsole: async () => {
        await this.requestEnvelope(
          "DELETE",
          `${toInstancePath(instanceName)}/console`,
        );
      },
    });

    const createFilesApi = (instanceName: string): InstanceFilesApi => ({
      get: async (path: string) => {
        const response = await this.requestTransport(
          "GET",
          `${toInstancePath(instanceName)}/files`,
          {
            query: { path },
          },
        );

        const headers = parseFileHeaders(response.headers);
        if ((headers.type ?? "").toLowerCase() === "directory") {
          const decoded = isIncusEnvelope(response.data)
            ? toStringArray(response.data.metadata)
            : [];
          return {
            stream: toReadableStream(new Uint8Array()),
            uid: headers.uid,
            gid: headers.gid,
            mode: headers.mode,
            type: headers.type,
            entries: decoded,
          };
        }

        if (response.status >= 400) {
          if (isIncusEnvelope(response.data)) {
            throw new IncusApiError(
              response.data.error ?? `Incus request failed (${response.status})`,
              response.status,
              {
                statusCode: response.data.status_code,
                errorCode: response.data.error_code,
                details: response.data.metadata,
              },
            );
          }

          throw new IncusApiError(`Incus request failed (${response.status})`, response.status, {
            details: response.data,
          });
        }

        return {
          stream: toReadableStream(toBytes(response.data)),
          uid: headers.uid,
          gid: headers.gid,
          mode: headers.mode,
          type: headers.type,
        };
      },
      put: async (path: string, options: InstanceFilePutOptions) => {
        const headers = new Headers();
        if (options.uid !== undefined) {
          headers.set("X-Incus-uid", String(options.uid));
        }

        if (options.gid !== undefined) {
          headers.set("X-Incus-gid", String(options.gid));
        }

        if (options.mode !== undefined) {
          headers.set("X-Incus-mode", options.mode.toString(8).padStart(4, "0"));
        }

        if (options.type) {
          headers.set("X-Incus-type", options.type);
        }

        if (options.writeMode) {
          headers.set("X-Incus-write", options.writeMode);
        }

        await this.requestEnvelope("POST", `${toInstancePath(instanceName)}/files`, {
          query: { path },
          body: options.content,
          headers,
        });
      },
      remove: async (path: string) => {
        await this.requestEnvelope("DELETE", `${toInstancePath(instanceName)}/files`, {
          query: { path },
        });
      },
      sftp: async () => {
        throw new Error("[Incus.ts] Instance SFTP helper is not implemented yet");
      },
    });

    const createSnapshotsApi = (instanceName: string): InstanceSnapshotsApi => ({
      names: async () => {
        const result = await this.requestEnvelope<unknown[]>("GET", toSnapshotPath(instanceName));
        return urlsToResourceNames(
          `/instances/${encodeURIComponent(instanceName)}/snapshots`,
          toStringArray(result.value),
        );
      },
      list: async () => {
        const result = await this.requestEnvelope<IncusRecord[]>("GET", toSnapshotPath(instanceName), {
          query: { recursion: 1 },
        });
        return toRecordArray(result.value);
      },
      get: async (snapshotName: string) => {
        const result = await this.requestEnvelope<IncusRecord>(
          "GET",
          toSnapshotPath(instanceName, snapshotName),
        );
        return { value: toRecord(result.value), etag: result.etag };
      },
      create: (snapshot: IncusRecord) => {
        return this.createOperationFromRequest("POST", toSnapshotPath(instanceName), {
          body: snapshot,
        });
      },
      copyFrom: async (
        _source: IncusClient,
        _snapshot: IncusRecord,
        _options?: IncusRecord,
      ) => createRemoteOperationFromTarget(null),
      rename: (snapshotName: string, request: IncusRecord) => {
        return this.createOperationFromRequest(
          "POST",
          toSnapshotPath(instanceName, snapshotName),
          {
            body: request,
          },
        );
      },
      migrate: (snapshotName: string, request: IncusRecord) => {
        return this.createOperationFromRequest(
          "POST",
          toSnapshotPath(instanceName, snapshotName),
          {
            body: request,
          },
        );
      },
      remove: (snapshotName: string) => {
        return this.createOperationFromRequest(
          "DELETE",
          toSnapshotPath(instanceName, snapshotName),
        );
      },
      update: (
        snapshotName: string,
        snapshot: IncusRecord,
        options: IncusMutationOptions = {},
      ) => {
        return this.createOperationFromRequest(
          "PUT",
          toSnapshotPath(instanceName, snapshotName),
          {
            body: snapshot,
            etag: options.etag,
          },
        );
      },
      restore: (snapshotName: string, options: { stateful?: boolean } = {}) => {
        return this.createOperationFromRequest("PUT", toInstancePath(instanceName), {
          body: {
            restore: snapshotName,
            ...(options.stateful !== undefined ? { stateful: options.stateful } : {}),
          },
        });
      },
    });

    const getInstance = async (
      instanceName: string,
      options: { full?: boolean } = {},
    ): Promise<IncusEntity<IncusRecord>> => {
      const result = await this.requestEnvelope<IncusRecord>(
        "GET",
        toInstancePath(instanceName),
        {
          query: options.full ? { recursion: 1 } : undefined,
        },
      );
      return { value: toRecord(result.value), etag: result.etag };
    };

    const updateInstance = (
      instanceName: string,
      instance: IncusRecord,
      options: IncusMutationOptions = {},
    ): IncusAwaitableOperation => {
      return this.createOperationFromRequest("PUT", toInstancePath(instanceName), {
        body: instance,
        etag: options.etag,
      });
    };

    const forkInstance = (
      instanceName: string,
      name: string,
      options: InstanceForkOptions = {},
    ): IncusAwaitableOperation => {
      const {
        fromSnapshot,
        sourceProject,
        live,
        instanceOnly,
        refresh,
        refreshExcludeOlder,
        allowInconsistent,
      } = options;
      const source = fromSnapshot
        ? `${instanceName}/${fromSnapshot}`
        : instanceName;
      const sourceRequest: IncusRecord = {
        type: "copy",
        source,
      };

      if (sourceProject) {
        sourceRequest.project = sourceProject;
      }

      if (live !== undefined) {
        sourceRequest.live = live;
      }

      if (instanceOnly !== undefined) {
        sourceRequest.instance_only = instanceOnly;
      }

      if (refresh !== undefined) {
        sourceRequest.refresh = refresh;
      }

      if (refreshExcludeOlder !== undefined) {
        sourceRequest.refresh_exclude_older = refreshExcludeOlder;
      }

      if (allowInconsistent !== undefined) {
        sourceRequest.allow_inconsistent = allowInconsistent;
      }

      return this.createOperationFromRequest(
        "POST",
        "/1.0/instances",
        {
          body: {
            name,
            source: sourceRequest,
          },
        },
      );
    };

    const renameInstance = (
      instanceName: string,
      request: IncusRecord,
    ): IncusAwaitableOperation => {
      return this.createOperationFromRequest("POST", toInstancePath(instanceName), { body: request });
    };

    const migrateInstance = (
      instanceName: string,
      request: IncusRecord,
    ): IncusAwaitableOperation => {
      return this.createOperationFromRequest("POST", toInstancePath(instanceName), { body: request });
    };

    const removeInstance = (instanceName: string): IncusAwaitableOperation => {
      return this.createOperationFromRequest("DELETE", toInstancePath(instanceName));
    };

    const rebuildInstance = (
      instanceName: string,
      request: IncusRecord,
    ): IncusAwaitableOperation => {
      return this.createOperationFromRequest("POST", `${toInstancePath(instanceName)}/rebuild`, {
        body: request,
      });
    };

    const stateInstance = async (instanceName: string): Promise<IncusEntity<IncusRecord>> => {
      const result = await this.requestEnvelope<IncusRecord>(
        "GET",
        `${toInstancePath(instanceName)}/state`,
      );
      return { value: toRecord(result.value), etag: result.etag };
    };

    const setStateInstance = (
      instanceName: string,
      state: IncusRecord,
      options: IncusMutationOptions = {},
    ): IncusAwaitableOperation => {
      return this.createOperationFromRequest("PUT", `${toInstancePath(instanceName)}/state`, {
        body: state,
        etag: options.etag,
      });
    };

    const accessInstance = async (instanceName: string): Promise<IncusRecord> => {
      const result = await this.requestEnvelope<IncusRecord>(
        "GET",
        `${toInstancePath(instanceName)}/access`,
      );
      return toRecord(result.value);
    };

    const execInstance = (
      instanceName: string,
      request: IncusRecord,
      options?: InstanceExecOptions,
    ): IncusExecProcess => {
      const path = `${toInstancePath(instanceName)}/exec`;
      const body = { ...request };
      const shouldAttachIO = this.shouldAttachExecIO(options) || this.shouldPipeExecIO(options);
      if (shouldAttachIO && !hasWaitForWebsocketFlag(body)) {
        body["wait-for-websocket"] = true;
      }

      const stdoutPipe = this.isPipeTarget(options?.stdout);
      const stderrPipe = this.isPipeTarget(options?.stderr);
      const stdoutQueue = createAsyncQueue<Uint8Array>();
      const stderrQueue = createAsyncQueue<Uint8Array>();
      const outputQueue = createAsyncQueue<IncusExecOutputChunk>();
      let stdoutOpen = stdoutPipe;
      let stderrOpen = stderrPipe;

      const closeOutputIfDone = () => {
        if (!stdoutOpen && !stderrOpen) {
          outputQueue.close();
        }
      };

      const closeStdout = () => {
        if (!stdoutOpen) {
          return;
        }

        stdoutOpen = false;
        stdoutQueue.close();
        closeOutputIfDone();
      };

      const closeStderr = () => {
        if (!stderrOpen) {
          return;
        }

        stderrOpen = false;
        stderrQueue.close();
        closeOutputIfDone();
      };

      const failAllQueues = (error: unknown) => {
        stdoutOpen = false;
        stderrOpen = false;
        stdoutQueue.fail(error);
        stderrQueue.fail(error);
        outputQueue.fail(error);
      };

      const operationReady = (async (): Promise<IncusOperation> => {
        const result = await this.requestEnvelope<IncusRecord>("POST", path, {
          body,
          signal: options?.signal,
        });
        const operation = this.createOperationFromEnvelopeResult("POST", path, result);

        if (shouldAttachIO) {
          const attached = await this.attachExecWebsockets(operation, body, options, result.value, {
            onStdoutChunk: stdoutPipe
              ? (chunk) => {
                stdoutQueue.push(chunk);
                outputQueue.push({ stream: "stdout", chunk });
              }
              : undefined,
            onStderrChunk: stderrPipe
              ? (chunk) => {
                stderrQueue.push(chunk);
                outputQueue.push({ stream: "stderr", chunk });
              }
              : undefined,
            onStdoutError: stdoutPipe
              ? (error) => {
                stdoutOpen = false;
                stdoutQueue.fail(error);
                closeOutputIfDone();
              }
              : undefined,
            onStderrError: stderrPipe
              ? (error) => {
                stderrOpen = false;
                stderrQueue.fail(error);
                closeOutputIfDone();
              }
              : undefined,
            onStdoutClose: stdoutPipe ? closeStdout : undefined,
            onStderrClose: stderrPipe ? closeStderr : undefined,
          });

          if (stdoutPipe && !attached.hasStdout) {
            closeStdout();
          }

          if (stderrPipe && !attached.hasStderr) {
            closeStderr();
          }
        } else {
          stdoutOpen = false;
          stderrOpen = false;
          stdoutQueue.close();
          stderrQueue.close();
          outputQueue.close();
        }

        if (!stdoutPipe) {
          stdoutOpen = false;
          stdoutQueue.close();
        }

        if (!stderrPipe) {
          stderrOpen = false;
          stderrQueue.close();
        }

        closeOutputIfDone();

        return operation;
      })().catch((error) => {
        failAllQueues(error);
        throw error;
      });

      const waitResult = async (
        waitOptions: IncusOperationWaitOptions = {},
      ): Promise<IncusExecResult> => {
        const operation = await operationReady;
        const opRecord = await operation.wait(waitOptions);
        await Promise.all([stdoutQueue.done, stderrQueue.done]);
        return toExecResult(opRecord);
      };

      let defaultResultPromise: Promise<IncusExecResult> | undefined;
      const getDefaultResultPromise = () => {
        defaultResultPromise ??= waitResult();
        return defaultResultPromise;
      };

      let operationId = "";
      void operationReady.then((operation) => {
        operationId = operation.id;
      }).catch(() => {
        // Best effort.
      });

      return {
        get id(): string {
          return operationId;
        },
        wait: (waitOptions: IncusOperationWaitOptions = {}) => (
          operationReady.then((operation) => operation.wait(waitOptions))
        ),
        cancel: () => operationReady.then((operation) => operation.cancel()),
        refresh: () => operationReady.then((operation) => operation.refresh()),
        websocket: (secret: string) => operationReady.then((operation) => operation.websocket(secret)),
        onUpdate: (handler: (operation: IncusRecord) => void) => (
          operationReady.then((operation) => operation.onUpdate(handler))
        ),
        stdout: stdoutQueue.iterable,
        stderr: stderrQueue.iterable,
        output: () => outputQueue.iterable,
        waitResult,
        then: <TResult1 = IncusExecResult, TResult2 = never>(
          onFulfilled?: ((value: IncusExecResult) => TResult1 | PromiseLike<TResult1>) | null,
          onRejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
        ): Promise<TResult1 | TResult2> => (
          getDefaultResultPromise().then(onFulfilled, onRejected)
        ),
        catch: <TResult = never>(
          onRejected?: ((reason: unknown) => TResult | PromiseLike<TResult>) | null,
        ): Promise<IncusExecResult | TResult> => (
          getDefaultResultPromise().catch(onRejected)
        ),
        finally: (onFinally?: (() => void) | null): Promise<IncusExecResult> => (
          getDefaultResultPromise().finally(onFinally ?? undefined)
        ),
        [Symbol.asyncIterator]: () => stdoutQueue.iterable[Symbol.asyncIterator](),
      } satisfies IncusExecProcess;
    };

    const consoleInstance = (
      instanceName: string,
      request: IncusRecord,
      options?: InstanceConsoleOptions,
    ): IncusAwaitableOperation => {
      return this.createOperationFromRequest(
        "POST",
        `${toInstancePath(instanceName)}/console`,
        {
          body: request,
          signal: options?.signal,
        },
      );
    };

    const metadataInstance = async (instanceName: string): Promise<IncusEntity<IncusRecord>> => {
      const result = await this.requestEnvelope<IncusRecord>(
        "GET",
        `${toInstancePath(instanceName)}/metadata`,
      );
      return { value: toRecord(result.value), etag: result.etag };
    };

    const updateMetadataInstance = async (
      instanceName: string,
      metadata: IncusRecord,
      options: IncusMutationOptions = {},
    ): Promise<void> => {
      await this.requestEnvelope("PUT", `${toInstancePath(instanceName)}/metadata`, {
        body: metadata,
        etag: options.etag,
      });
    };

    const debugMemoryInstance = async (
      instanceName: string,
      format = "elf",
    ): Promise<ReadableStream<Uint8Array>> => {
      const result = await this.requestBinary(
        "GET",
        `${toInstancePath(instanceName)}/debug/memory`,
        {
          query: {
            format,
            "instance-type": "virtual-machine",
          },
        },
      );
      return toReadableStream(result.value);
    };

    const createInstanceHandle = (instanceName: string): InstanceApi => {
      const logs = createLogsApi(instanceName);
      const files = createFilesApi(instanceName);
      const snapshots = createSnapshotsApi(instanceName);

      return {
        name: instanceName,
        get: (options = {}) => getInstance(instanceName, options),
        fork: (name: string, options: InstanceForkOptions = {}) => (
          forkInstance(instanceName, name, options)
        ),
        update: (instance: IncusRecord, options: IncusMutationOptions = {}) => (
          updateInstance(instanceName, instance, options)
        ),
        rename: (request: IncusRecord) => renameInstance(instanceName, request),
        migrate: (request: IncusRecord) => migrateInstance(instanceName, request),
        remove: () => removeInstance(instanceName),
        rebuild: (request: IncusRecord) => rebuildInstance(instanceName, request),
        rebuildFromImage: async (
          _source: IncusImageClient,
          _image: IncusRecord,
          _request: IncusRecord,
        ) => createRemoteOperationFromTarget(null),
        restore: (snapshotName: string, options: { stateful?: boolean } = {}) => (
          snapshots.restore(snapshotName, options)
        ),
        state: () => stateInstance(instanceName),
        setState: (state: IncusRecord, options: IncusMutationOptions = {}) => (
          setStateInstance(instanceName, state, options)
        ),
        access: () => accessInstance(instanceName),
        exec: (request: IncusRecord, options?: InstanceExecOptions) => (
          execInstance(instanceName, request, options)
        ),
        console: (request: IncusRecord, options?: InstanceConsoleOptions) => (
          consoleInstance(instanceName, request, options)
        ),
        consoleDynamic: async () => {
          throw new Error("[Incus.ts] Dynamic console attach is not implemented yet");
        },
        metadata: () => metadataInstance(instanceName),
        updateMetadata: (metadata: IncusRecord, options: IncusMutationOptions = {}) => (
          updateMetadataInstance(instanceName, metadata, options)
        ),
        debugMemory: (format = "elf") => debugMemoryInstance(instanceName, format),
        logs,
        files,
        templates: createNotImplementedProxy(["instances", "templates"]) as InstanceTemplatesApi,
        snapshots,
        backups: createNotImplementedProxy(["instances", "backups"]) as InstanceBackupsApi,
      };
    };

    return createApiWithFallback<InstancesApi>("instances", {
      names: async (options = {}) => {
        if (options.allProjects) {
          const query: Record<string, string | number | boolean | undefined> = {
            recursion: 1,
            "all-projects": "true",
          };
          if (options.type) {
            query["instance-type"] = options.type;
          }

          const result = await this.requestEnvelope<IncusRecord[]>(
            "GET",
            "/1.0/instances",
            { query },
          );
          const records = toRecordArray(result.value);
          const grouped: Record<string, string[]> = {};
          for (const record of records) {
            const project = typeof record.project === "string"
              ? record.project
              : "default";
            const name = typeof record.name === "string" ? record.name : undefined;
            if (!name) {
              continue;
            }

            grouped[project] ??= [];
            grouped[project].push(name);
          }

          return grouped;
        }

        const query: Record<string, string | number | boolean | undefined> = {};
        if (options.type) {
          query["instance-type"] = options.type;
        }

        const result = await this.requestEnvelope<unknown[]>("GET", "/1.0/instances", {
          query,
        });
        return urlsToResourceNames("/instances", toStringArray(result.value));
      },
      list: async (options = {}) => {
        const query: Record<string, string | number | boolean | undefined> = {
          recursion: options.full ? 2 : 1,
        };

        if (options.type) {
          query["instance-type"] = options.type;
        }

        if (options.allProjects) {
          query["all-projects"] = "true";
        }

        if (options.filter && options.filter.length > 0) {
          query.filter = parseFilters(options.filter);
        }

        const result = await this.requestEnvelope<IncusRecord[]>(
          "GET",
          "/1.0/instances",
          { query },
        );
        return toRecordArray(result.value);
      },
      instance: (name: string) => createInstanceHandle(name),
      create: (instance: IncusRecord) => {
        return this.createOperationFromRequest("POST", "/1.0/instances", { body: instance });
      },
      createFromImage: async (
        _source: IncusImageClient,
        _image: IncusRecord,
        _request: IncusRecord,
      ) => createRemoteOperationFromTarget(null),
      createFromBackup: (args: IncusRecord) => {
        return this.createOperationFromRequest("POST", "/1.0/instances", { body: args });
      },
      copyFrom: async (
        _source: IncusClient,
        _instance: IncusRecord,
        _options?: IncusRecord,
      ) => createRemoteOperationFromTarget(null),
      updateMany: (state: IncusRecord, options: IncusMutationOptions = {}) => {
        return this.createOperationFromRequest("PUT", "/1.0/instances", {
          body: state,
          etag: options.etag,
        });
      },
    });
  }
}

export class Incus {
  static async connect(
    endpoint: string,
    options: IncusConnectionOptions = {},
  ): Promise<IncusClient> {
    const normalized = normalizeEndpoint(endpoint);
    const descriptor: IncusTransportDescriptor = { kind: "incus", endpoint: normalized };
    return new IncusClient(
      new FetchTransport(descriptor, options),
      normalized,
      options,
      {},
      "incus",
    );
  }

  static async connectHttp(
    options: IncusHttpConnectOptions = {},
  ): Promise<IncusClient> {
    const endpoint = normalizeEndpoint(options.endpoint ?? "https://custom.socket");
    const descriptor: IncusTransportDescriptor = { kind: "incus-http", endpoint };
    return new IncusClient(
      new FetchTransport(descriptor, options),
      endpoint,
      options,
      {},
      "incus-http",
    );
  }

  static async connectUnix(
    options: IncusUnixConnectOptions = {},
  ): Promise<IncusClient> {
    const socketPath = options.socketPath ?? "/var/lib/incus/unix.socket";
    const endpoint = `unix://${socketPath}`;
    const descriptor: IncusTransportDescriptor = {
      kind: "incus-unix",
      endpoint,
      socketPath,
    };

    return new IncusClient(
      new UnixSocketTransport(descriptor, options),
      endpoint,
      options,
      {},
      "incus-unix",
    );
  }

  static async connectPublic(
    endpoint: string,
    options: IncusConnectionOptions = {},
  ): Promise<IncusImageClient> {
    const normalized = normalizeEndpoint(endpoint);
    const descriptor: IncusTransportDescriptor = {
      kind: "incus-public",
      endpoint: normalized,
    };
    return new IncusImageClient(
      new FetchTransport(descriptor, options),
      normalized,
      options,
      {},
      "incus-public",
    );
  }

  static async connectSimpleStreams(
    endpoint: string,
    options: IncusSimpleStreamsOptions = {},
  ): Promise<IncusImageClient> {
    const normalized = normalizeEndpoint(endpoint);
    const descriptor: IncusTransportDescriptor = {
      kind: "simple-streams",
      endpoint: normalized,
    };
    return new IncusImageClient(
      new FetchTransport(descriptor, options),
      normalized,
      options,
      {},
      "simple-streams",
    );
  }

  static fromTransport(
    endpoint: string,
    transport: IncusTransport,
    options: IncusConnectionOptions = {},
  ): IncusClient {
    return new IncusClient(transport, normalizeEndpoint(endpoint), options);
  }

  static fromImageTransport(
    endpoint: string,
    transport: IncusTransport,
    options: IncusConnectionOptions = {},
  ): IncusImageClient {
    return new IncusImageClient(transport, normalizeEndpoint(endpoint), options);
  }
}
