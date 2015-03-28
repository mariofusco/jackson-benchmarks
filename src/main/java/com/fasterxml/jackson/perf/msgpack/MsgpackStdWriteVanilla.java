package com.fasterxml.jackson.perf.msgpack;

import org.msgpack.jackson.dataformat.msgpack.MessagePackFactory;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Scope;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.perf.WritePerfBasicJackson;
import com.fasterxml.jackson.perf.model.MediaItem;

@State(Scope.Group) // Thread, Group or Benchmark
public class MsgpackStdWriteVanilla
    extends WritePerfBasicJackson<MediaItem>
{
    public MsgpackStdWriteVanilla() {
        super(new ObjectMapper(new MessagePackFactory()));
    }
}
