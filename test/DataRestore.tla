---------------------------- MODULE DataRestore ----------------------------

(***************************************************************************
数据恢复模型：

源节点复制文件到目标节点，目标节点开始服务。此时，源节点崩溃，数据丢失，需要
实现源节点能够从目标节点恢复数据。

注意，此时正在服务的节点，可能是经过多次迁移的。在迁移时，也可能经过源节点。
但是，迁移时源节点还没有数据丢失。
 ***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

\* 所有文件的集合
CONSTANTS Filenames

\* 所有节点的集合
CONSTANTS Nodes

CONSTANTS MetaId, RestoreId

CONSTANTS Nil

CONSTANTS CMD_GET_FILES_REQ, CMD_GET_FILES_REP

ProcIds == {MetaId, RestoreId}

AllCommands == {CMD_GET_FILES_REQ, CMD_GET_FILES_REP}

SeqPathSet == [Nat \ {0} -> [from: Nodes, to: Nodes]]
SinglePathSet == {seq \in SeqPathSet : LET seqlen == Len(seq)
                                       IN  /\ \A i \in 1..seqlen-1 : seq[i].to = seq[i+1].from
                                           /\ seq[seqlen].from = seq[seqlen].to}

InuseSgwSet == {x[Len(x)].to : x \in SinglePathSet}

(***************************************************************************
--algorithm DataRestore {

variables   file_records \in [Nat \ {0} -> [filename: Filenames, from: Nodes]]; \* 文件记录列表
            migration_records \in SinglePathSet; \* 数据迁移记录
            command = (MetaId :> Nil) @@ (RestoreId :> Nil);
            restore_files = {}; \* meta 返回的文件列表
            ret_src_sgw = {};
            inuse_sgw = migration_records[Len(migration_records)].to;

define {

    TypeOK == /\ file_records \in [Nat \ {0} -> [filename: Filenames, from: Nodes]]
              /\ migration_records \in SinglePathSet
              /\ command \in [ProcIds -> {Nil} \cup AllCommands]
              /\ restore_files \in SUBSET Filenames
              /\ ret_src_sgw \in SUBSET Nodes
              /\ inuse_sgw \in Nodes

    Range(f) == {f[x] : x \in DOMAIN f}
    
    SourceSgw(dst, ranges) == {x1.from : x1 \in {x \in Range(ranges) : /\ x.from # x.to
                                                                       /\ x.to = dst}}
    
    AllFiles(records, sgws) == {x1.filename : x1 \in {x \in Range(records) : x.from \in sgws}}
    
    SgwSourceSet(sgw, records) == {x.from : x \in Range(records)} \cup {y.to : y \in Range(records)}
    
    \* 从 meta 接收到的文件列表和正确的文件列表相同
    Inv1 == pc[RestoreId] = "R2" =>
                /\ restore_files = AllFiles(file_records, SgwSourceSet(inuse_sgw, migration_records))
};

procedure get_source_sgw(dst = Nil)
variables src_sgw = {}; dst_sgw = dst; tmp_sgw = Nil;
{
L0:
    ret_src_sgw := {};
L1:
    while (dst_sgw \in Nodes) {
        ret_src_sgw := ret_src_sgw \cup {dst_sgw};
        tmp_sgw := SourceSgw(dst_sgw, migration_records);
        if (tmp_sgw # {}) {
            L2:
                with (s \in tmp_sgw) {
                    dst_sgw := s;
                    tmp_sgw := tmp_sgw \ {s};
                };
                src_sgw := src_sgw \cup tmp_sgw;
        } else {
            if (src_sgw = {}) {
                dst_sgw := Nil;
            } else {
                with (s \in src_sgw) {
                    dst_sgw := s;
                    src_sgw := src_sgw \ {s};
                };
            };
        };
    };
    return;
};

procedure get_files(sgws = {})
{
G0:
    restore_files := AllFiles(file_records, sgws);
    return;
};

process (meta \in {MetaId})
{
M0:
    while (TRUE) {
        await command[self] = CMD_GET_FILES_REQ; \* 处理接收到的获取恢复文件列表
        call get_source_sgw(inuse_sgw);
        M1:
            call get_files(ret_src_sgw);
        M2:
            command[RestoreId] := CMD_GET_FILES_REP;
    };
};

process (restore \in {RestoreId})
variables file = Nil;
{
R0:
    command[MetaId] := CMD_GET_FILES_REQ; \* 发送请求
R1:
    await command[self] = CMD_GET_FILES_REP; \* 同步等待响应
R2:
    skip;
};

};
 ***************************************************************************)
\* BEGIN TRANSLATION
VARIABLES file_records, migration_records, command, restore_files, 
          ret_src_sgw, inuse_sgw, pc, stack

(* define statement *)
TypeOK == /\ file_records \in [Nat \ {0} -> [filename: Filenames, from: Nodes]]
          /\ migration_records \in SinglePathSet
          /\ command \in [ProcIds -> {Nil} \cup AllCommands]
          /\ restore_files \in SUBSET Filenames
          /\ ret_src_sgw \in SUBSET Nodes
          /\ inuse_sgw \in Nodes

Range(f) == {f[x] : x \in DOMAIN f}

SourceSgw(dst, ranges) == {x1.from : x1 \in {x \in Range(ranges) : /\ x.from # x.to
                                                                   /\ x.to = dst}}

AllFiles(records, sgws) == {x1.filename : x1 \in {x \in Range(records) : x.from \in sgws}}

SgwSourceSet(sgw, records) == {x.from : x \in Range(records)} \cup {y.to : y \in Range(records)}


Inv1 == pc[RestoreId] = "R2" =>
            /\ restore_files = AllFiles(file_records, SgwSourceSet(inuse_sgw, migration_records))

VARIABLES dst, src_sgw, dst_sgw, tmp_sgw, sgws, file

vars == << file_records, migration_records, command, restore_files, 
           ret_src_sgw, inuse_sgw, pc, stack, dst, src_sgw, dst_sgw, tmp_sgw, 
           sgws, file >>

ProcSet == ({MetaId}) \cup ({RestoreId})

Init == (* Global variables *)
        /\ file_records \in [Nat \ {0} -> [filename: Filenames, from: Nodes]]
        /\ migration_records \in SinglePathSet
        /\ command = (MetaId :> Nil) @@ (RestoreId :> Nil)
        /\ restore_files = {}
        /\ ret_src_sgw = {}
        /\ inuse_sgw = migration_records[Len(migration_records)].to
        (* Procedure get_source_sgw *)
        /\ dst = [ self \in ProcSet |-> Nil]
        /\ src_sgw = [ self \in ProcSet |-> {}]
        /\ dst_sgw = [ self \in ProcSet |-> dst[self]]
        /\ tmp_sgw = [ self \in ProcSet |-> Nil]
        (* Procedure get_files *)
        /\ sgws = [ self \in ProcSet |-> {}]
        (* Process restore *)
        /\ file = [self \in {RestoreId} |-> Nil]
        /\ stack = [self \in ProcSet |-> << >>]
        /\ pc = [self \in ProcSet |-> CASE self \in {MetaId} -> "M0"
                                        [] self \in {RestoreId} -> "R0"]

L0(self) == /\ pc[self] = "L0"
            /\ ret_src_sgw' = {}
            /\ pc' = [pc EXCEPT ![self] = "L1"]
            /\ UNCHANGED << file_records, migration_records, command, 
                            restore_files, inuse_sgw, stack, dst, src_sgw, 
                            dst_sgw, tmp_sgw, sgws, file >>

L1(self) == /\ pc[self] = "L1"
            /\ IF dst_sgw[self] \in Nodes
                  THEN /\ ret_src_sgw' = (ret_src_sgw \cup {dst_sgw[self]})
                       /\ tmp_sgw' = [tmp_sgw EXCEPT ![self] = SourceSgw(dst_sgw[self], migration_records)]
                       /\ IF tmp_sgw'[self] # {}
                             THEN /\ pc' = [pc EXCEPT ![self] = "L2"]
                                  /\ UNCHANGED << src_sgw, dst_sgw >>
                             ELSE /\ IF src_sgw[self] = {}
                                        THEN /\ dst_sgw' = [dst_sgw EXCEPT ![self] = Nil]
                                             /\ UNCHANGED src_sgw
                                        ELSE /\ \E s \in src_sgw[self]:
                                                  /\ dst_sgw' = [dst_sgw EXCEPT ![self] = s]
                                                  /\ src_sgw' = [src_sgw EXCEPT ![self] = src_sgw[self] \ {s}]
                                  /\ pc' = [pc EXCEPT ![self] = "L1"]
                       /\ UNCHANGED << stack, dst >>
                  ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                       /\ src_sgw' = [src_sgw EXCEPT ![self] = Head(stack[self]).src_sgw]
                       /\ dst_sgw' = [dst_sgw EXCEPT ![self] = Head(stack[self]).dst_sgw]
                       /\ tmp_sgw' = [tmp_sgw EXCEPT ![self] = Head(stack[self]).tmp_sgw]
                       /\ dst' = [dst EXCEPT ![self] = Head(stack[self]).dst]
                       /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                       /\ UNCHANGED ret_src_sgw
            /\ UNCHANGED << file_records, migration_records, command, 
                            restore_files, inuse_sgw, sgws, file >>

L2(self) == /\ pc[self] = "L2"
            /\ \E s \in tmp_sgw[self]:
                 /\ dst_sgw' = [dst_sgw EXCEPT ![self] = s]
                 /\ tmp_sgw' = [tmp_sgw EXCEPT ![self] = tmp_sgw[self] \ {s}]
            /\ src_sgw' = [src_sgw EXCEPT ![self] = src_sgw[self] \cup tmp_sgw'[self]]
            /\ pc' = [pc EXCEPT ![self] = "L1"]
            /\ UNCHANGED << file_records, migration_records, command, 
                            restore_files, ret_src_sgw, inuse_sgw, stack, dst, 
                            sgws, file >>

get_source_sgw(self) == L0(self) \/ L1(self) \/ L2(self)

G0(self) == /\ pc[self] = "G0"
            /\ restore_files' = AllFiles(file_records, sgws[self])
            /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
            /\ sgws' = [sgws EXCEPT ![self] = Head(stack[self]).sgws]
            /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
            /\ UNCHANGED << file_records, migration_records, command, 
                            ret_src_sgw, inuse_sgw, dst, src_sgw, dst_sgw, 
                            tmp_sgw, file >>

get_files(self) == G0(self)

M0(self) == /\ pc[self] = "M0"
            /\ command[self] = CMD_GET_FILES_REQ
            /\ /\ dst' = [dst EXCEPT ![self] = inuse_sgw]
               /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "get_source_sgw",
                                                        pc        |->  "M1",
                                                        src_sgw   |->  src_sgw[self],
                                                        dst_sgw   |->  dst_sgw[self],
                                                        tmp_sgw   |->  tmp_sgw[self],
                                                        dst       |->  dst[self] ] >>
                                                    \o stack[self]]
            /\ src_sgw' = [src_sgw EXCEPT ![self] = {}]
            /\ dst_sgw' = [dst_sgw EXCEPT ![self] = dst'[self]]
            /\ tmp_sgw' = [tmp_sgw EXCEPT ![self] = Nil]
            /\ pc' = [pc EXCEPT ![self] = "L0"]
            /\ UNCHANGED << file_records, migration_records, command, 
                            restore_files, ret_src_sgw, inuse_sgw, sgws, file >>

M1(self) == /\ pc[self] = "M1"
            /\ /\ sgws' = [sgws EXCEPT ![self] = ret_src_sgw]
               /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "get_files",
                                                        pc        |->  "M2",
                                                        sgws      |->  sgws[self] ] >>
                                                    \o stack[self]]
            /\ pc' = [pc EXCEPT ![self] = "G0"]
            /\ UNCHANGED << file_records, migration_records, command, 
                            restore_files, ret_src_sgw, inuse_sgw, dst, 
                            src_sgw, dst_sgw, tmp_sgw, file >>

M2(self) == /\ pc[self] = "M2"
            /\ command' = [command EXCEPT ![RestoreId] = CMD_GET_FILES_REP]
            /\ pc' = [pc EXCEPT ![self] = "M0"]
            /\ UNCHANGED << file_records, migration_records, restore_files, 
                            ret_src_sgw, inuse_sgw, stack, dst, src_sgw, 
                            dst_sgw, tmp_sgw, sgws, file >>

meta(self) == M0(self) \/ M1(self) \/ M2(self)

R0(self) == /\ pc[self] = "R0"
            /\ command' = [command EXCEPT ![MetaId] = CMD_GET_FILES_REQ]
            /\ pc' = [pc EXCEPT ![self] = "R1"]
            /\ UNCHANGED << file_records, migration_records, restore_files, 
                            ret_src_sgw, inuse_sgw, stack, dst, src_sgw, 
                            dst_sgw, tmp_sgw, sgws, file >>

R1(self) == /\ pc[self] = "R1"
            /\ command[self] = CMD_GET_FILES_REP
            /\ pc' = [pc EXCEPT ![self] = "R2"]
            /\ UNCHANGED << file_records, migration_records, command, 
                            restore_files, ret_src_sgw, inuse_sgw, stack, dst, 
                            src_sgw, dst_sgw, tmp_sgw, sgws, file >>

R2(self) == /\ pc[self] = "R2"
            /\ TRUE
            /\ pc' = [pc EXCEPT ![self] = "Done"]
            /\ UNCHANGED << file_records, migration_records, command, 
                            restore_files, ret_src_sgw, inuse_sgw, stack, dst, 
                            src_sgw, dst_sgw, tmp_sgw, sgws, file >>

restore(self) == R0(self) \/ R1(self) \/ R2(self)

Next == (\E self \in ProcSet: get_source_sgw(self) \/ get_files(self))
           \/ (\E self \in {MetaId}: meta(self))
           \/ (\E self \in {RestoreId}: restore(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION

=============================================================================
\* Modification History
\* Last modified Fri Oct 19 17:46:18 CST 2018 by hurq
\* Created Wed Oct 17 10:03:05 CST 2018 by hurq
