/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.client.avro;

<<<<<<< HEAD
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.flume.FlumeException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestSpoolingFileLineReader {
  private static String completedSuffix = ".COMPLETE";
  private static int bufferMaxLineLength = 500;
  private static int bufferMaxLines = 30;

  private File tmpDir;

=======
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.LineDeserializer;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class TestSpoolingFileLineReader {

  Logger logger = LoggerFactory.getLogger(TestSpoolingFileLineReader.class);

  private static String completedSuffix =
      SpoolDirectorySourceConfigurationConstants.DEFAULT_SPOOLED_FILE_SUFFIX;
  private static int bufferMaxLineLength = 500;

  private File tmpDir;

  static String bodyAsString(Event event) {
    return new String(event.getBody());
  }

  static List<String> bodiesAsStrings(List<Event> events) {
    List<String> bodies = Lists.newArrayListWithCapacity(events.size());
    for (Event event : events) {
      bodies.add(bodyAsString(event));
    }
    return bodies;
  }

  private ReliableSpoolingFileEventReader getParser(int maxLineLength) {
    Context ctx = new Context();
    ctx.put(LineDeserializer.MAXLINE_KEY, Integer.toString(maxLineLength));
    ReliableSpoolingFileEventReader parser;
    try {
      parser = new ReliableSpoolingFileEventReader.Builder()
          .spoolDirectory(tmpDir)
          .completedSuffix(completedSuffix)
          .deserializerContext(ctx)
          .build();
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
    return parser;
  }

  private ReliableSpoolingFileEventReader getParser() {
    return getParser(bufferMaxLineLength);
  }

  private FileFilter directoryFilter() {
    return new FileFilter() {
      public boolean accept(File candidate) {
        if (candidate.isDirectory()) {
          return false;
        }
        return true;
      }
    };
  }

>>>>>>> refs/remotes/apache/trunk
  @Before
  public void setUp() {
    tmpDir = Files.createTempDir();
  }

  @After
  public void tearDown() {
    for (File f : tmpDir.listFiles()) {
<<<<<<< HEAD
=======
      if (f.isDirectory()) {
        for (File sdf : f.listFiles()) {
          sdf.delete();
        }
      }
>>>>>>> refs/remotes/apache/trunk
      f.delete();
    }
    tmpDir.delete();
  }

  @Test
<<<<<<< HEAD
  /** Create three multi-line files then read them back out. Ensures that
   * files are accessed in correct order and that lines are read correctly
   * from files. */
=======
  // Create three multi-line files then read them back out. Ensures that
  // files are accessed in correct order and that lines are read correctly
  // from files.
>>>>>>> refs/remotes/apache/trunk
  public void testBasicSpooling() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    File f3 = new File(tmpDir.getAbsolutePath() + "/file3");

    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);
    Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);
    Files.write("file3line1\nfile3line2\n", f3, Charsets.UTF_8);

<<<<<<< HEAD
    SpoolingFileLineReader reader = new SpoolingFileLineReader(
        tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);

    List<String> out = Lists.newArrayList();
    for (int i = 0; i < 6; i++) {
      out.add(reader.readLine());
      reader.commit();
=======
    ReliableSpoolingFileEventReader parser = getParser();

    List<String> out = Lists.newArrayList();
    for (int i = 0; i < 6; i++) {
      logger.info("At line {}", i);
      String body = bodyAsString(parser.readEvent());
      logger.debug("Seen event with body: {}", body);
      out.add(body);
      parser.commit();
>>>>>>> refs/remotes/apache/trunk
    }
    // Make sure we got every line
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));
    assertTrue(out.contains("file2line1"));
    assertTrue(out.contains("file2line2"));
    assertTrue(out.contains("file3line1"));
    assertTrue(out.contains("file3line2"));

<<<<<<< HEAD
    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles());
=======
    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles(directoryFilter()));
>>>>>>> refs/remotes/apache/trunk

    assertEquals(3, outFiles.size());

    // Make sure files 1 and 2 have been processed and file 3 is still open
    assertTrue(outFiles.contains(new File(tmpDir + "/file1" + completedSuffix)));
    assertTrue(outFiles.contains(new File(tmpDir + "/file2" + completedSuffix)));
    assertTrue(outFiles.contains(new File(tmpDir + "/file3")));
  }

  @Test
<<<<<<< HEAD
  /** Make sure this works when there are initially no files */
  public void testInitiallyEmptyDirectory() throws IOException {

    SpoolingFileLineReader reader = new SpoolingFileLineReader(
        tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);

    assertNull(reader.readLine());
    assertEquals(0, reader.readLines(10).size());
=======
  // Make sure this works when there are initially no files
  public void testInitiallyEmptyDirectory() throws IOException {

    ReliableSpoolingFileEventReader parser = getParser();

    assertNull(parser.readEvent());
    assertEquals(0, parser.readEvents(10).size());
>>>>>>> refs/remotes/apache/trunk

    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);

<<<<<<< HEAD
    List<String> out = reader.readLines(2);
    reader.commit();
=======
    List<String> out = bodiesAsStrings(parser.readEvents(2));
    parser.commit();
>>>>>>> refs/remotes/apache/trunk

    // Make sure we got all of the first file
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);

<<<<<<< HEAD
    reader.readLine(); // force roll
    reader.commit();

    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles());
=======
    parser.readEvent(); // force roll
    parser.commit();

    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles(directoryFilter()));
>>>>>>> refs/remotes/apache/trunk

    assertEquals(2, outFiles.size());
    assertTrue(
        outFiles.contains(new File(tmpDir + "/file1" + completedSuffix)));
    assertTrue(outFiles.contains(new File(tmpDir + "/file2")));
  }

  @Test(expected = IllegalStateException.class)
<<<<<<< HEAD
  /** Ensures that file immutability is enforced. */
  public void testFileChangesDuringRead() throws IOException {
    File tmpDir1 = Files.createTempDir();
    File f1 = new File(tmpDir1.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);

    SpoolingFileLineReader reader1 =
        new SpoolingFileLineReader(tmpDir1, completedSuffix, bufferMaxLines, bufferMaxLineLength);

    List<String> out = Lists.newArrayList();
    out.addAll(reader1.readLines(2));
    reader1.commit();
=======
  // Ensures that file immutability is enforced.
  public void testFileChangesDuringRead() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);

    ReliableSpoolingFileEventReader parser1 = getParser();

    List<String> out = Lists.newArrayList();
    out.addAll(bodiesAsStrings(parser1.readEvents(2)));
    parser1.commit();
>>>>>>> refs/remotes/apache/trunk

    assertEquals(2, out.size());
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));

    Files.append("file1line3\n", f1, Charsets.UTF_8);

<<<<<<< HEAD
    out.add(reader1.readLine());
    reader1.commit();
    out.add(reader1.readLine());
    reader1.commit();
  }


  /** Test when a competing destination file is found, but it matches,
   *  and we are on a Windows machine. */
=======
    out.add(bodyAsString(parser1.readEvent()));
    parser1.commit();
    out.add(bodyAsString(parser1.readEvent()));
    parser1.commit();
  }


  // Test when a competing destination file is found, but it matches,
  //  and we are on a Windows machine.
>>>>>>> refs/remotes/apache/trunk
  @Test
  public void testDestinationExistsAndSameFileWindows() throws IOException {
    System.setProperty("os.name", "Some version of Windows");

    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    File f1Completed = new File(tmpDir.getAbsolutePath() + "/file1" +
        completedSuffix);

    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);
    Files.write("file1line1\nfile1line2\n", f1Completed, Charsets.UTF_8);

<<<<<<< HEAD
    SpoolingFileLineReader reader =
        new SpoolingFileLineReader(tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);
=======
    ReliableSpoolingFileEventReader parser = getParser();
>>>>>>> refs/remotes/apache/trunk

    List<String> out = Lists.newArrayList();

    for (int i = 0; i < 2; i++) {
<<<<<<< HEAD
      out.add(reader.readLine());
      reader.commit();
=======
      out.add(bodyAsString(parser.readEvent()));
      parser.commit();
>>>>>>> refs/remotes/apache/trunk
    }

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);

    for (int i = 0; i < 2; i++) {
<<<<<<< HEAD
      out.add(reader.readLine());
      reader.commit();
=======
      out.add(bodyAsString(parser.readEvent()));
      parser.commit();
>>>>>>> refs/remotes/apache/trunk
    }

    // Make sure we got every line
    assertEquals(4, out.size());
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));
    assertTrue(out.contains("file2line1"));
    assertTrue(out.contains("file2line2"));

    // Make sure original is deleted
<<<<<<< HEAD
    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles());
=======
    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles(directoryFilter()));
>>>>>>> refs/remotes/apache/trunk
    assertEquals(2, outFiles.size());
    assertTrue(outFiles.contains(new File(tmpDir + "/file2")));
    assertTrue(outFiles.contains(
        new File(tmpDir + "/file1" + completedSuffix)));
  }

<<<<<<< HEAD
  /** Test when a competing destination file is found, but it matches,
   *  and we are not on a Windows machine. */
=======
  // Test when a competing destination file is found, but it matches,
  // and we are not on a Windows machine.
>>>>>>> refs/remotes/apache/trunk
  @Test(expected = IllegalStateException.class)
  public void testDestinationExistsAndSameFileNotOnWindows() throws IOException {
    System.setProperty("os.name", "Some version of Linux");

    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    File f1Completed = new File(tmpDir.getAbsolutePath() + "/file1" +
        completedSuffix);

    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);
    Files.write("file1line1\nfile1line2\n", f1Completed, Charsets.UTF_8);

<<<<<<< HEAD
    SpoolingFileLineReader reader =
        new SpoolingFileLineReader(tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);
=======
    ReliableSpoolingFileEventReader parser = getParser();
>>>>>>> refs/remotes/apache/trunk

    List<String> out = Lists.newArrayList();

    for (int i = 0; i < 2; i++) {
<<<<<<< HEAD
      out.add(reader.readLine());
      reader.commit();
=======
      out.add(bodyAsString(parser.readEvent()));
      parser.commit();
>>>>>>> refs/remotes/apache/trunk
    }

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);

    for (int i = 0; i < 2; i++) {
<<<<<<< HEAD
      out.add(reader.readLine());
      reader.commit();
=======
      out.add(bodyAsString(parser.readEvent()));
      parser.commit();
>>>>>>> refs/remotes/apache/trunk
    }

    // Not reached
  }

  @Test
<<<<<<< HEAD
  /** Test a basic case where a commit is missed. */
=======
  // Test a basic case where a commit is missed.
>>>>>>> refs/remotes/apache/trunk
  public void testBasicCommitFailure() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n" +
                "file1line9\nfile1line10\nfile1line11\nfile1line12\n",
                f1, Charsets.UTF_8);

<<<<<<< HEAD
    SpoolingFileLineReader reader = new SpoolingFileLineReader(
        tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);

    List<String> out1 = reader.readLines(4);
=======
    ReliableSpoolingFileEventReader parser = getParser();

    List<String> out1 = bodiesAsStrings(parser.readEvents(4));
>>>>>>> refs/remotes/apache/trunk
    assertTrue(out1.contains("file1line1"));
    assertTrue(out1.contains("file1line2"));
    assertTrue(out1.contains("file1line3"));
    assertTrue(out1.contains("file1line4"));

<<<<<<< HEAD
    List<String> out2 = reader.readLines(4);
=======
    List<String> out2 = bodiesAsStrings(parser.readEvents(4));
>>>>>>> refs/remotes/apache/trunk
    assertTrue(out2.contains("file1line1"));
    assertTrue(out2.contains("file1line2"));
    assertTrue(out2.contains("file1line3"));
    assertTrue(out2.contains("file1line4"));

<<<<<<< HEAD
    reader.commit();

    List<String> out3 = reader.readLines(4);
=======
    parser.commit();

    List<String> out3 = bodiesAsStrings(parser.readEvents(4));
>>>>>>> refs/remotes/apache/trunk
    assertTrue(out3.contains("file1line5"));
    assertTrue(out3.contains("file1line6"));
    assertTrue(out3.contains("file1line7"));
    assertTrue(out3.contains("file1line8"));

<<<<<<< HEAD
    reader.commit();
    List<String> out4 = reader.readLines(10);
=======
    parser.commit();
    List<String> out4 = bodiesAsStrings(parser.readEvents(4));
>>>>>>> refs/remotes/apache/trunk
    assertEquals(4, out4.size());
    assertTrue(out4.contains("file1line9"));
    assertTrue(out4.contains("file1line10"));
    assertTrue(out4.contains("file1line11"));
    assertTrue(out4.contains("file1line12"));
  }

  @Test
<<<<<<< HEAD
  /** Test a case where a commit is missed and the buffer size shrinks. */
=======
  // Test a case where a commit is missed and the buffer size shrinks.
>>>>>>> refs/remotes/apache/trunk
  public void testBasicCommitFailureAndBufferSizeChanges() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n" +
                "file1line9\nfile1line10\nfile1line11\nfile1line12\n",
                f1, Charsets.UTF_8);

<<<<<<< HEAD
    SpoolingFileLineReader reader = new SpoolingFileLineReader(
        tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);

    List<String> out1 = reader.readLines(5);
=======
    ReliableSpoolingFileEventReader parser = getParser();

    List<String> out1 = bodiesAsStrings(parser.readEvents(5));
>>>>>>> refs/remotes/apache/trunk
    assertTrue(out1.contains("file1line1"));
    assertTrue(out1.contains("file1line2"));
    assertTrue(out1.contains("file1line3"));
    assertTrue(out1.contains("file1line4"));
    assertTrue(out1.contains("file1line5"));

<<<<<<< HEAD
    List<String> out2 = reader.readLines(2);
    assertTrue(out2.contains("file1line1"));
    assertTrue(out2.contains("file1line2"));

    reader.commit();
    List<String> out3 = reader.readLines(2);
    assertTrue(out3.contains("file1line3"));
    assertTrue(out3.contains("file1line4"));

    reader.commit();
    List<String> out4 = reader.readLines(2);
    assertTrue(out4.contains("file1line5"));
    assertTrue(out4.contains("file1line6"));

    reader.commit();
    List<String> out5 = reader.readLines(2);
    assertTrue(out5.contains("file1line7"));
    assertTrue(out5.contains("file1line8"));

    reader.commit();

    List<String> out6 = reader.readLines(15);
=======
    List<String> out2 = bodiesAsStrings(parser.readEvents(2));
    assertTrue(out2.contains("file1line1"));
    assertTrue(out2.contains("file1line2"));

    parser.commit();
    List<String> out3 = bodiesAsStrings(parser.readEvents(2));
    assertTrue(out3.contains("file1line3"));
    assertTrue(out3.contains("file1line4"));

    parser.commit();
    List<String> out4 = bodiesAsStrings(parser.readEvents(2));
    assertTrue(out4.contains("file1line5"));
    assertTrue(out4.contains("file1line6"));

    parser.commit();
    List<String> out5 = bodiesAsStrings(parser.readEvents(2));
    assertTrue(out5.contains("file1line7"));
    assertTrue(out5.contains("file1line8"));

    parser.commit();

    List<String> out6 = bodiesAsStrings(parser.readEvents(15));
>>>>>>> refs/remotes/apache/trunk
    assertTrue(out6.contains("file1line9"));
    assertTrue(out6.contains("file1line10"));
    assertTrue(out6.contains("file1line11"));
    assertTrue(out6.contains("file1line12"));
  }

<<<<<<< HEAD
  /** Test when a competing destination file is found and it does not match. */
=======
  // Test when a competing destination file is found and it does not match.
>>>>>>> refs/remotes/apache/trunk
  @Test(expected = IllegalStateException.class)
  public void testDestinationExistsAndDifferentFile() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    File f1Completed =
        new File(tmpDir.getAbsolutePath() + "/file1" + completedSuffix);

    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);
    Files.write("file1line1\nfile1XXXe2\n", f1Completed, Charsets.UTF_8);

<<<<<<< HEAD
    SpoolingFileLineReader reader =
        new SpoolingFileLineReader(tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);
=======
    ReliableSpoolingFileEventReader parser = getParser();
>>>>>>> refs/remotes/apache/trunk

    List<String> out = Lists.newArrayList();

    for (int i = 0; i < 2; i++) {
<<<<<<< HEAD
      out.add(reader.readLine());
      reader.commit();
=======
      out.add(bodyAsString(parser.readEvent()));
      parser.commit();
>>>>>>> refs/remotes/apache/trunk
    }

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);

    for (int i = 0; i < 2; i++) {
<<<<<<< HEAD
      out.add(reader.readLine());
      reader.commit();
=======
      out.add(bodyAsString(parser.readEvent()));
      parser.commit();
>>>>>>> refs/remotes/apache/trunk
    }
    // Not reached
  }


<<<<<<< HEAD
  /**
   * Empty files should be treated the same as other files and rolled.
   */
  @Test
  public void testBehaviorWithEmptyFile() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    Files.touch(f1);

    SpoolingFileLineReader reader =
        new SpoolingFileLineReader(tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
=======
   // Empty files should be treated the same as other files and rolled.
  @Test
  public void testBehaviorWithEmptyFile() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file0");
    Files.touch(f1);

    ReliableSpoolingFileEventReader parser = getParser();

    File f2 = new File(tmpDir.getAbsolutePath() + "/file1");
>>>>>>> refs/remotes/apache/trunk
    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f2, Charsets.UTF_8);

<<<<<<< HEAD
    List<String> out = reader.readLines(8); // Expect to skip over first file
    reader.commit();
=======
    // Expect to skip over first file
    List<String> out = bodiesAsStrings(parser.readEvents(8));

    parser.commit();
>>>>>>> refs/remotes/apache/trunk
    assertEquals(8, out.size());

    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));
    assertTrue(out.contains("file1line3"));
    assertTrue(out.contains("file1line4"));
    assertTrue(out.contains("file1line5"));
    assertTrue(out.contains("file1line6"));
    assertTrue(out.contains("file1line7"));
    assertTrue(out.contains("file1line8"));

<<<<<<< HEAD
    assertNull(reader.readLine());

    // Make sure original is deleted
    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles());
    assertEquals(2, outFiles.size());
    assertTrue(outFiles.contains(
        new File(tmpDir + "/file1" + completedSuffix)));
    assertTrue(outFiles.contains(
        new File(tmpDir + "/file2" + completedSuffix)));
=======
    assertNull(parser.readEvent());

    // Make sure original is deleted
    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles(directoryFilter()));
    assertEquals(2, outFiles.size());
    assertTrue("Outfiles should have file0 & file1: " + outFiles,
        outFiles.contains(new File(tmpDir + "/file0" + completedSuffix)));
    assertTrue("Outfiles should have file0 & file1: " + outFiles,
        outFiles.contains(new File(tmpDir + "/file1" + completedSuffix)));
>>>>>>> refs/remotes/apache/trunk
  }

  @Test
  public void testBatchedReadsWithinAFile() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

<<<<<<< HEAD
    SpoolingFileLineReader reader = new SpoolingFileLineReader(tmpDir,
        completedSuffix, bufferMaxLines, bufferMaxLineLength);

    List<String> out = reader.readLines(5);
    reader.commit();
=======
    ReliableSpoolingFileEventReader parser = getParser();

    List<String> out = bodiesAsStrings(parser.readEvents(5));
    parser.commit();
>>>>>>> refs/remotes/apache/trunk

    // Make sure we got every line
    assertEquals(5, out.size());
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));
    assertTrue(out.contains("file1line3"));
    assertTrue(out.contains("file1line4"));
    assertTrue(out.contains("file1line5"));
  }

  @Test
  public void testBatchedReadsAcrossFileBoundary() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

<<<<<<< HEAD
    SpoolingFileLineReader reader = new SpoolingFileLineReader(tmpDir,
        completedSuffix, bufferMaxLines, bufferMaxLineLength);
    List<String> out1 = reader.readLines(5);
    reader.commit();
=======
    ReliableSpoolingFileEventReader parser = getParser();

    List<String> out1 = bodiesAsStrings(parser.readEvents(5));
    parser.commit();
>>>>>>> refs/remotes/apache/trunk

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    Files.write("file2line1\nfile2line2\nfile2line3\nfile2line4\n" +
        "file2line5\nfile2line6\nfile2line7\nfile2line8\n",
        f2, Charsets.UTF_8);

<<<<<<< HEAD
    List<String> out2 = reader.readLines(5);
    reader.commit();
    List<String> out3 = reader.readLines(5);
    reader.commit();
=======
    List<String> out2 = bodiesAsStrings(parser.readEvents(5));
    parser.commit();
    List<String> out3 = bodiesAsStrings(parser.readEvents(5));
    parser.commit();
>>>>>>> refs/remotes/apache/trunk

    // Should have first 5 lines of file1
    assertEquals(5, out1.size());
    assertTrue(out1.contains("file1line1"));
    assertTrue(out1.contains("file1line2"));
    assertTrue(out1.contains("file1line3"));
    assertTrue(out1.contains("file1line4"));
    assertTrue(out1.contains("file1line5"));

    // Should have 3 remaining lines of file1
    assertEquals(3, out2.size());
    assertTrue(out2.contains("file1line6"));
    assertTrue(out2.contains("file1line7"));
    assertTrue(out2.contains("file1line8"));

    // Should have first 5 lines of file2
    assertEquals(5, out3.size());
    assertTrue(out3.contains("file2line1"));
    assertTrue(out3.contains("file2line2"));
    assertTrue(out3.contains("file2line3"));
    assertTrue(out3.contains("file2line4"));
    assertTrue(out3.contains("file2line5"));

    // file1 should be moved now
<<<<<<< HEAD
    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles());
=======
    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles(directoryFilter()));
>>>>>>> refs/remotes/apache/trunk
    assertEquals(2, outFiles.size());
    assertTrue(outFiles.contains(
        new File(tmpDir + "/file1" + completedSuffix)));
    assertTrue(outFiles.contains(new File(tmpDir + "/file2")));
  }

  @Test
<<<<<<< HEAD
  /** Test the case where we read finish reading and fully commit a file, then
   *  the directory is empty. */
=======
  // Test the case where we read finish reading and fully commit a file, then
  //  the directory is empty.
>>>>>>> refs/remotes/apache/trunk
  public void testEmptyDirectoryAfterCommittingFile() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);

<<<<<<< HEAD
    SpoolingFileLineReader reader = new SpoolingFileLineReader(
        tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);

    List<String> allLines = reader.readLines(2);
    assertEquals(2, allLines.size());
    reader.commit();

    List<String> empty = reader.readLines(10);
    assertEquals(0, empty.size());
  }

  @Test(expected = FlumeException.class)
  /** When a line violates the character limit, we should throw an exception,
   * even if the buffer limit is not actually exceeded. */
  public void testLineExceedsMaxLineLengthButNotBufferSize() throws IOException {
    final int maxLineLength = 15;
=======
    ReliableSpoolingFileEventReader parser = getParser();

    List<String> allLines = bodiesAsStrings(parser.readEvents(2));
    assertEquals(2, allLines.size());
    parser.commit();

    List<String> empty = bodiesAsStrings(parser.readEvents(10));
    assertEquals(0, empty.size());
  }

  // When a line violates the character limit we should truncate it.
  @Test
  public void testLineExceedsMaxLineLength() throws IOException {
    final int maxLineLength = 12;
>>>>>>> refs/remotes/apache/trunk
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n" +
<<<<<<< HEAD
                "reallyreallyreallyreallyreallyLongfile1line9\n" +
                "file1line10\nfile1line11\nfile1line12\nfile1line13\n",
                f1, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(
        tmpDir, completedSuffix, bufferMaxLines, maxLineLength);

    List<String> out1 = reader.readLines(5);
=======
                "reallyreallyreallyreallyLongLineHerefile1line9\n" +
                "file1line10\nfile1line11\nfile1line12\nfile1line13\n",
                f1, Charsets.UTF_8);

    ReliableSpoolingFileEventReader parser = getParser(maxLineLength);

    List<String> out1 = bodiesAsStrings(parser.readEvents(5));
>>>>>>> refs/remotes/apache/trunk
    assertTrue(out1.contains("file1line1"));
    assertTrue(out1.contains("file1line2"));
    assertTrue(out1.contains("file1line3"));
    assertTrue(out1.contains("file1line4"));
    assertTrue(out1.contains("file1line5"));
<<<<<<< HEAD
    reader.commit();

    reader.readLines(5);
  }

  @Test(expected = FlumeException.class)
  /** When a line is larger than the entire buffer, we should definitely throw
   * an exception. */
  public void testLineExceedsBufferSize() throws IOException {
    final int maxLineLength = 15;
    final int bufferMaxLines = 10;

    // String slightly longer than buffer
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < (bufferMaxLines + 1) * maxLineLength; i++) {
      sb.append('x');
    }

    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    Files.write(sb.toString() + "\n",
                f1, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(
        tmpDir, completedSuffix, bufferMaxLines, maxLineLength);

    reader.readLines(5);
  }

  @Test
  /** When a line is larger than the entire buffer, we should definitely throw
   * an exception. */
  public void testCallsFailWhenReaderDisabled() throws IOException {
    final int maxLineLength = 15;
    final int bufferMaxLines = 10;

    // String slightly longer than buffer
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < (bufferMaxLines + 1) * maxLineLength; i++) {
      sb.append('x');
    }

    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    Files.write(sb.toString() + "\n",
                f1, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(
        tmpDir, completedSuffix, bufferMaxLines, maxLineLength);

    boolean exceptionOnRead = false;
    boolean illegalStateOnReadLine = false;
    boolean illegalStateOnReadLines = false;
    boolean illegalStateOnCommit = false;
    try {
      reader.readLines(5);
    } catch (FlumeException e) {
      exceptionOnRead = true;
    }
    try {
      reader.readLine();
    } catch (IllegalStateException e) {
      illegalStateOnReadLine = true;
    }
    try {
      reader.readLines(5);
    } catch (IllegalStateException e) {
      illegalStateOnReadLines = true;
    }
    try {
      reader.commit();
    } catch (IllegalStateException e) {
      illegalStateOnCommit = true;
    }

    Assert.assertTrue("Got FlumeException when reading long line.",
        exceptionOnRead);
    Assert.assertTrue("Got IllegalStateException when reading line.",
        illegalStateOnReadLine);
    Assert.assertTrue("Got IllegalStateException when reading lines.",
        illegalStateOnReadLines);
    Assert.assertTrue("Got IllegalStateException when commiting",
        illegalStateOnCommit);

  }



=======
    parser.commit();

    List<String> out2 = bodiesAsStrings(parser.readEvents(4));
    assertTrue(out2.contains("file1line6"));
    assertTrue(out2.contains("file1line7"));
    assertTrue(out2.contains("file1line8"));
    assertTrue(out2.contains("reallyreally"));
    parser.commit();

    List<String> out3 = bodiesAsStrings(parser.readEvents(5));
    assertTrue(out3.contains("reallyreally"));
    assertTrue(out3.contains("LongLineHere"));
    assertTrue(out3.contains("file1line9"));
    assertTrue(out3.contains("file1line10"));
    assertTrue(out3.contains("file1line11"));
    parser.commit();

    List<String> out4 = bodiesAsStrings(parser.readEvents(5));
    assertTrue(out4.contains("file1line12"));
    assertTrue(out4.contains("file1line13"));
    assertEquals(2, out4.size());
    parser.commit();

    assertEquals(0, parser.readEvents(5).size());
  }

>>>>>>> refs/remotes/apache/trunk
  @Test
  public void testNameCorrespondsToLatestRead() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

<<<<<<< HEAD
    SpoolingFileLineReader reader = new SpoolingFileLineReader(tmpDir,
        completedSuffix, bufferMaxLines, bufferMaxLineLength);
    reader.readLines(5);
    reader.commit();

    assertNotNull(reader.getLastFileRead());
    assertTrue(reader.getLastFileRead().endsWith("file1"));
=======
    ReliableSpoolingFileEventReader parser = getParser();
    parser.readEvents(5);
    parser.commit();

    assertNotNull(parser.getLastFileRead());
    assertTrue(parser.getLastFileRead().endsWith("file1"));
>>>>>>> refs/remotes/apache/trunk

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    Files.write("file2line1\nfile2line2\nfile2line3\nfile2line4\n" +
        "file2line5\nfile2line6\nfile2line7\nfile2line8\n",
        f2, Charsets.UTF_8);

<<<<<<< HEAD
    reader.readLines(5);
    reader.commit();
    assertNotNull(reader.getLastFileRead());
    assertTrue(reader.getLastFileRead().endsWith("file1"));

    reader.readLines(5);
    reader.commit();
    assertNotNull(reader.getLastFileRead());
    assertTrue(reader.getLastFileRead().endsWith("file2"));

    reader.readLines(5);
    assertTrue(reader.getLastFileRead().endsWith("file2"));

    reader.readLines(5);
    assertTrue(reader.getLastFileRead().endsWith("file2"));
  }
=======
    parser.readEvents(5);
    parser.commit();
    assertNotNull(parser.getLastFileRead());
    assertTrue(parser.getLastFileRead().endsWith("file1"));

    parser.readEvents(5);
    parser.commit();
    assertNotNull(parser.getLastFileRead());
    assertTrue(parser.getLastFileRead().endsWith("file2"));

    parser.readEvents(5);
    assertTrue(parser.getLastFileRead().endsWith("file2"));

    parser.readEvents(5);
    assertTrue(parser.getLastFileRead().endsWith("file2"));
  }

>>>>>>> refs/remotes/apache/trunk
}
