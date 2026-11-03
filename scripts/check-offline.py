#!/usr/bin/env python3
"""Compile with already cached JARs when Gradle networking/socket access is restricted.
This checks Java/API compatibility against the cached Spark version, not the pinned runtime.
"""

from pathlib import Path
import subprocess
import os

root = Path(__file__).resolve().parents[1]
os.chdir(root)
cache = Path.home() / ".gradle/caches/modules-2/files-2.1"


# Select the highest cached version of each artifact, avoiding conflicting duplicate JARs.
def version_key(s):
    import re

    return tuple((0, int(x)) if x.isdigit() else (1, x) for x in re.split(r"(\d+)", s))


jars = []
for group in cache.iterdir():
    for artifact in group.iterdir():
        versions = sorted(
            artifact.iterdir(), key=lambda p: version_key(p.name), reverse=True
        )
        # Match cached Spark's Scala Jackson module (2.21) rather than mixing Jackson minors.
        if group.name.startswith("com.fasterxml.jackson"):
            aligned = [
                p for p in versions if p.name == "2.21" or p.name.startswith("2.21.")
            ]
            if aligned:
                versions = aligned
        if artifact.name == "slf4j-simple":
            continue
        for version in versions:
            candidates = [
                p
                for p in version.rglob("*.jar")
                if not p.name.endswith(("-sources.jar", "-javadoc.jar", "-tests.jar"))
            ]
            if candidates:
                jars.extend(candidates)
                break
out = root / "build/offline-classes"
out.mkdir(parents=True, exist_ok=True)
classpath = os.pathsep.join(str(p) for p in jars)
sources = [p for p in root.glob("*/src/**/*.java")]
argfile = root / "build/javac.args"
argfile.write_text(
    "\n".join(
        ["--release", "17", "-proc:none", "-cp", classpath, "-d", str(out)]
        + [str(p) for p in sources]
    )
)
subprocess.run(["javac", "@" + str(argfile)], check=True)
for name in ["dev.sparkbench.runner.SelfTest", "dev.sparkbench.dataset.EventsSelfTest"]:
    javaargs = root / "build/java.args"
    javaargs.write_text("\n".join(["-cp", str(out) + os.pathsep + classpath, name]))
    subprocess.run(["java", "@" + str(javaargs)], check=True)
print(
    "Offline compilation and self-tests passed. Live Spark/Kubernetes validation is still required."
)
