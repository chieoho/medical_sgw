--------------------------- MODULE DataMigration ---------------------------

(***************************************************************************
将一个数据中心存储的文件，迁移（拷贝）到另外一个数据中心。数据迁移需要满足以下的
要求：

1. 数据保证完整性
2. 可以进行数据恢复
3. 客户端、存储网关切换到新的数据中心
4. 可以进行在线迁移，存储网关、元数据服务、客户端同时在线也可以进行数据迁移
5. 可以进行离线迁移，存储网关、元数据服务拒绝服务客户端
6. 用户使用时，只需要选择源数据中心和目标数据中心，不要求输入其他信息

为了设计上的可行性，人为地加入以下的约束：

1. 正在进行数据迁移时，不再客户端的上传请求
2. 数据迁移完成之后，不再处理客户端的任何请求，告知客户端数据迁移的目标数据中心
3. 正在进行数据迁移时，目标数据中心不处理客户端的任何请求
 ***************************************************************************)

EXTENDS Integers, Sequences, TLC

\* 进程集合
CONSTANTS SrcSgw, DstSgw, TransferClient

\* 需要传输的文件序列，序列的元素是文件的大小，例如  <<3, 4, 2>>
CONSTANTS Files

\* 角色
CONSTANTS Leader, Follower

\* 迁移状态：未迁移，正在迁移，已经迁移完成
CONSTANTS S_NORMAL, S_MIGRATING, S_MIGRATED

\* 传输进程和源 sgw 交互的命令
CONSTANTS   CMD_MIGRATION_START,
            CMD_MIGRATION_STOP,
            CMD_MIGRATION_FINISHED,
            CMD_MIGRATION_CANCEL

\* 表示空值
CONSTANTS Nil

ASSUME /\ Len(Files) < 5
       /\ \A x \in DOMAIN Files: Files[x] > 0

RECURSIVE SeqSum(_)
SeqSum(seq) == IF Len(seq) = 0 THEN 0 ELSE seq[1] + SeqSum(Tail(seq))

(***************************************************************************
--algorithm DataMigration {

variables   command = (TransferClient :> Nil) @@ (SrcSgw :> Nil); \* 传输进程和源 sgw 通信的消息
            role = (SrcSgw :> Leader) @@ (DstSgw :> Follower); \* 源和目标 sgw 的角色
            srcqueue = << >>; \* 源 sgw 的发送队列
            dstqueue = << >>; \* 目标 sgw 的接收队列
            transfer_files = Files; \* 源 sgw 需要传输的文件序列，序列的每个元素是文件的大小，形如 <<4, 2, 3>>

define {
    MigrationCommands == {CMD_MIGRATION_START, CMD_MIGRATION_STOP, CMD_MIGRATION_FINISHED, CMD_MIGRATION_CANCEL}
    
    RoleSet == {Leader, Follower}

    TypeOK == /\ command \in [{TransferClient, SrcSgw} -> {Nil} \cup MigrationCommands]
              /\ role \in [{SrcSgw, DstSgw} -> {Leader, Follower}]
              /\ \A x \in DOMAIN srcqueue: srcqueue[x] > 0
              /\ \A x \in DOMAIN dstqueue: dstqueue[x] > 0

    \* 源 sgw 和目标 sgw 不能同时服务
    Inv1 == ~(role[SrcSgw] = Leader /\ role[DstSgw] = Leader)
    
    \* 源 sgw 的发送队列和目标 sgw 的接收队列的内容是一样的
    Inv2 == /\ Len(srcqueue) = Len(dstqueue)
            /\ \A f \in 1..Len(srcqueue): srcqueue[f] = dstqueue[f]

};

macro get_file(src_files, file) {
    file := Head(src_files);
    src_files := Tail(src_files);
};

\* 单个文件传输，可能会传输多次，也可能传输一次
procedure transfer_file(size = 0)
{
P1:
    while (size > 0) {
        with (n \in 1..size) {
            size := size - n;
            srcqueue := Append(srcqueue, n);
            dstqueue := Append(dstqueue, n);
        };
    };
    return;
};

\* 实际上传输文件的客户端
process (c \in {TransferClient})
variables   cur_file = Nil; \* 当前传输的文件
{
T1:
    while (Len(transfer_files) > 0) {
        either {
            \* 模拟文件传输过程
            await command[self] = CMD_MIGRATION_START;
            get_file(transfer_files, cur_file);
            call transfer_file(cur_file);
            T2:
                command[SrcSgw] := CMD_MIGRATION_FINISHED;
        } or {
            \* 模拟接收到“数据迁移停止”的命令
            await command[self] = CMD_MIGRATION_STOP;
            goto T3; \* 停止文件传输
        };
    };
T3:
    skip;
};

process (sgw1 \in {SrcSgw})
variables state = S_NORMAL; \* 源 sgw 的迁移状态
{
S1:
    while (state \in {S_NORMAL, S_MIGRATING}) {
        either {
            \* 模拟接收到用户的数据迁移命令，准备进行数据迁移
            state := S_MIGRATING;
            command[TransferClient] := CMD_MIGRATION_START;
        } or {
            \* 数据正在进行传输
            skip;
        } or {
            \* 模拟数据传输完成
            await command[self] = CMD_MIGRATION_FINISHED;
            state := S_MIGRATED;
            role[self] := Follower;
        } or {
            \* 模拟取消数据迁移
            await command[TransferClient] = CMD_MIGRATION_START;
            command[TransferClient] := CMD_MIGRATION_CANCEL;
        };
    };
};

process (sgw2 \in {DstSgw})
{
D1:
    while (TRUE) {
        either {
            \* 接收数据，源 sgw 发送的数据已经在 dstqueue 中了
            skip;
        } or {
            \* 数据迁移取消，目标 sgw 不可用
            role[self] := Follower;
        } or {
            \* 有客户端建立新的连接
            if (role[self] # Leader) {
                skip;
            };
        } or {
            \* 数据迁移完成
            await role[SrcSgw] = Follower;
            role[self] := Leader;
        };
    };
};

}
 ***************************************************************************)
\* BEGIN TRANSLATION
VARIABLES command, role, srcqueue, dstqueue, transfer_files, pc, stack

(* define statement *)
MigrationCommands == {CMD_MIGRATION_START, CMD_MIGRATION_STOP, CMD_MIGRATION_FINISHED, CMD_MIGRATION_CANCEL}

RoleSet == {Leader, Follower}

TypeOK == /\ command \in [{TransferClient, SrcSgw} -> {Nil} \cup MigrationCommands]
          /\ role \in [{SrcSgw, DstSgw} -> {Leader, Follower}]
          /\ \A x \in DOMAIN srcqueue: srcqueue[x] > 0
          /\ \A x \in DOMAIN dstqueue: dstqueue[x] > 0


Inv1 == ~(role[SrcSgw] = Leader /\ role[DstSgw] = Leader)


Inv2 == /\ Len(srcqueue) = Len(dstqueue)
        /\ \A f \in 1..Len(srcqueue): srcqueue[f] = dstqueue[f]

VARIABLES size, cur_file, state

vars == << command, role, srcqueue, dstqueue, transfer_files, pc, stack, size, 
           cur_file, state >>

ProcSet == ({TransferClient}) \cup ({SrcSgw}) \cup ({DstSgw})

Init == (* Global variables *)
        /\ command = (TransferClient :> Nil) @@ (SrcSgw :> Nil)
        /\ role = (SrcSgw :> Leader) @@ (DstSgw :> Follower)
        /\ srcqueue = << >>
        /\ dstqueue = << >>
        /\ transfer_files = Files
        (* Procedure transfer_file *)
        /\ size = [ self \in ProcSet |-> 0]
        (* Process c *)
        /\ cur_file = [self \in {TransferClient} |-> Nil]
        (* Process sgw1 *)
        /\ state = [self \in {SrcSgw} |-> S_NORMAL]
        /\ stack = [self \in ProcSet |-> << >>]
        /\ pc = [self \in ProcSet |-> CASE self \in {TransferClient} -> "T1"
                                        [] self \in {SrcSgw} -> "S1"
                                        [] self \in {DstSgw} -> "D1"]

P1(self) == /\ pc[self] = "P1"
            /\ IF size[self] > 0
                  THEN /\ \E n \in 1..size[self]:
                            /\ size' = [size EXCEPT ![self] = size[self] - n]
                            /\ srcqueue' = Append(srcqueue, n)
                            /\ dstqueue' = Append(dstqueue, n)
                       /\ pc' = [pc EXCEPT ![self] = "P1"]
                       /\ stack' = stack
                  ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                       /\ size' = [size EXCEPT ![self] = Head(stack[self]).size]
                       /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                       /\ UNCHANGED << srcqueue, dstqueue >>
            /\ UNCHANGED << command, role, transfer_files, cur_file, state >>

transfer_file(self) == P1(self)

T1(self) == /\ pc[self] = "T1"
            /\ IF Len(transfer_files) > 0
                  THEN /\ \/ /\ command[self] = CMD_MIGRATION_START
                             /\ cur_file' = [cur_file EXCEPT ![self] = Head(transfer_files)]
                             /\ transfer_files' = Tail(transfer_files)
                             /\ /\ size' = [size EXCEPT ![self] = cur_file'[self]]
                                /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "transfer_file",
                                                                         pc        |->  "T2",
                                                                         size      |->  size[self] ] >>
                                                                     \o stack[self]]
                             /\ pc' = [pc EXCEPT ![self] = "P1"]
                          \/ /\ command[self] = CMD_MIGRATION_STOP
                             /\ pc' = [pc EXCEPT ![self] = "T3"]
                             /\ UNCHANGED <<transfer_files, stack, size, cur_file>>
                  ELSE /\ pc' = [pc EXCEPT ![self] = "T3"]
                       /\ UNCHANGED << transfer_files, stack, size, cur_file >>
            /\ UNCHANGED << command, role, srcqueue, dstqueue, state >>

T2(self) == /\ pc[self] = "T2"
            /\ command' = [command EXCEPT ![SrcSgw] = CMD_MIGRATION_FINISHED]
            /\ pc' = [pc EXCEPT ![self] = "T1"]
            /\ UNCHANGED << role, srcqueue, dstqueue, transfer_files, stack, 
                            size, cur_file, state >>

T3(self) == /\ pc[self] = "T3"
            /\ TRUE
            /\ pc' = [pc EXCEPT ![self] = "Done"]
            /\ UNCHANGED << command, role, srcqueue, dstqueue, transfer_files, 
                            stack, size, cur_file, state >>

c(self) == T1(self) \/ T2(self) \/ T3(self)

S1(self) == /\ pc[self] = "S1"
            /\ IF state[self] \in {S_NORMAL, S_MIGRATING}
                  THEN /\ \/ /\ state' = [state EXCEPT ![self] = S_MIGRATING]
                             /\ command' = [command EXCEPT ![TransferClient] = CMD_MIGRATION_START]
                             /\ role' = role
                          \/ /\ TRUE
                             /\ UNCHANGED <<command, role, state>>
                          \/ /\ command[self] = CMD_MIGRATION_FINISHED
                             /\ state' = [state EXCEPT ![self] = S_MIGRATED]
                             /\ role' = [role EXCEPT ![self] = Follower]
                             /\ UNCHANGED command
                          \/ /\ command[TransferClient] = CMD_MIGRATION_START
                             /\ command' = [command EXCEPT ![TransferClient] = CMD_MIGRATION_CANCEL]
                             /\ UNCHANGED <<role, state>>
                       /\ pc' = [pc EXCEPT ![self] = "S1"]
                  ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                       /\ UNCHANGED << command, role, state >>
            /\ UNCHANGED << srcqueue, dstqueue, transfer_files, stack, size, 
                            cur_file >>

sgw1(self) == S1(self)

D1(self) == /\ pc[self] = "D1"
            /\ \/ /\ TRUE
                  /\ role' = role
               \/ /\ role' = [role EXCEPT ![self] = Follower]
               \/ /\ IF role[self] # Leader
                        THEN /\ TRUE
                        ELSE /\ TRUE
                  /\ role' = role
               \/ /\ role[SrcSgw] = Follower
                  /\ role' = [role EXCEPT ![self] = Leader]
            /\ pc' = [pc EXCEPT ![self] = "D1"]
            /\ UNCHANGED << command, srcqueue, dstqueue, transfer_files, stack, 
                            size, cur_file, state >>

sgw2(self) == D1(self)

Next == (\E self \in ProcSet: transfer_file(self))
           \/ (\E self \in {TransferClient}: c(self))
           \/ (\E self \in {SrcSgw}: sgw1(self))
           \/ (\E self \in {DstSgw}: sgw2(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION


\* 正在传输的文件大小和已经接收的文件大小的总数是不变的
Inv3 == SeqSum(srcqueue) + size[TransferClient] + SeqSum(transfer_files) = SeqSum(Files)

=============================================================================
\* Modification History
\* Last modified Wed Oct 17 10:11:36 CST 2018 by hurq
\* Created Fri Oct 12 09:16:29 CST 2018 by hurq
