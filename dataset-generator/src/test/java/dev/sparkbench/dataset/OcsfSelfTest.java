package dev.sparkbench.dataset;

import dev.sparkbench.core.Config;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;

import java.nio.file.*;
import java.util.*;

public final class OcsfSelfTest {
    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    public static void main(String[] args) throws Exception {
        Config c = Config.load(Path.of("config/ocsf.properties"));
        c.validate();
        OcsfCatalog cat = OcsfCatalog.INSTANCE;
        OcsfEvents events = new OcsfEvents(c);
        var schema = EventTable.generationSchema(c);
        check(cat.classes.length == 86, "all concrete OCSF classes");
        check(schema.length() == 207, "200 scalar + 5 benchmark + map + generation bucket");
        check(
                c.get("dataset.eventWeight.process_activity").equals("10"),
                "weights include resolved");
        check(!c.all().containsKey("config.includes"), "worker configs contain resolved weights");
        var serializer = ExpressionEncoder.apply(schema).createSerializer();
        int uid = schema.fieldIndex("class_uid"),
                activity = schema.fieldIndex("activity_id"),
                type = schema.fieldIndex("type_uid");
        int attrs = schema.fieldIndex("attributes"),
                version = schema.fieldIndex("metadata__version");
        int samples = 0;
        Set<String> seenTypes = new HashSet<>();
        long maps = 0;
        for (var cls : cat.classes) {
            for (String aid : cls.byPath.get("activity_id").enumKeys) {
                long n = samples++;
                Row row = events.rowForClass(n, cls, Long.valueOf(aid));
                serializer.apply(row);
                check(row.getLong(uid) == cls.uid, "class_uid");
                check(row.getLong(activity) == Long.parseLong(aid), "activity_id");
                check(
                        row.getLong(type) == cls.uid * 100L + Long.parseLong(aid),
                        "type_uid classification formula");
                check(row.getString(version).equals("1.9.0"), "schema version");
                check(row.getString(4).equals(cls.name), "eventtype identity");
                seenTypes.add(cls.name + ":" + aid);
                for (var entry : row.<String, String>getJavaMap(attrs).entrySet()) {
                    check(
                            cls.byPath.containsKey(entry.getKey()),
                            "map key exists in OCSF: " + entry.getKey());
                    check(entry.getValue() != null, "non-null map value");
                    maps++;
                }
                check(
                        row.equals(events.rowForClass(n, cls, Long.valueOf(aid))),
                        "deterministic row values");
            }
        }
        check(maps > samples, "real overflow fields populated");
        long process = 0, script = 0;
        Set<Integer> observed = new HashSet<>();
        for (int i = 0; i < 300000; i++) {
            var cls = events.eventClass(i);
            observed.add(cls.uid);
            if (cls.name.equals("process_activity")) process++;
            if (cls.name.equals("script_activity")) script++;
        }
        double ratio = (double) process / script;
        check(ratio > 9.4 && ratio < 10.6, "10:1 class weight distribution: " + ratio);
        check(observed.size() == 86, "every enabled class is sampled");
        Config changed = c.with("dataset.eventWeight.process_activity", "11");
        check(
                !Generator.inputFingerprint(c).equals(Generator.inputFingerprint(changed)),
                "weights invalidate input fingerprint");
        try {
            new OcsfEvents(c.with("dataset.eventWeight.misspelled_class", "1"));
            throw new AssertionError("Unknown weight accepted");
        } catch (IllegalArgumentException expected) {
        }
        Config only = c.with("dataset.ocsf.defaultEventWeight", "0");
        for (String key : c.prefix("dataset.eventWeight.").keySet())
            only = only.with("dataset.eventWeight." + key, "0");
        try {
            new OcsfEvents(only);
            throw new AssertionError("All-zero weights accepted");
        } catch (IllegalArgumentException expected) {
        }
        OcsfEvents processOnly =
                new OcsfEvents(only.with("dataset.eventWeight.process_activity", "1"));
        for (int i = 0; i < 100; i++)
            check(processOnly.eventClass(i).uid == 1007, "zero weight excludes other classes");
        Path temporary = Files.createTempDirectory("ocsf-effective-config-");
        try {
            Path file = temporary.resolve("job.properties");
            c.store(file);
            check(
                    Config.load(file).get("dataset.eventWeight.script_activity").equals("1"),
                    "standalone effective config reload");
        } finally {
            try (var files = Files.walk(temporary)) {
                for (Path f : files.sorted(Comparator.reverseOrder()).toList()) Files.delete(f);
            }
        }
        System.out.println(
                "OCSF self-test passed: "
                        + cat.classes.length
                        + " classes, "
                        + seenTypes.size()
                        + " class/activity types, 200 promoted fields, "
                        + maps
                        + " schema-backed map values; observed Process/Script ratio="
                        + ratio);
    }
}
