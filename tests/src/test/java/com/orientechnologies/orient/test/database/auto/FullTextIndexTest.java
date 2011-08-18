/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = { "index" }, sequential = true)
public class FullTextIndexTest {
	private static final int		TOT		= 1000;
	private ODatabaseDocumentTx	database;
	private static final String	TEXT	= "Jay Glenn Miner (May 31, 1932 – June 20, 1994), was a famous integrated circuit designer, known primarily for his "
																				+ "work in multimedia chips and as the 'father of the Amiga'[1]. He received a BS in EECS from "
																				+ "UC Berkeley in 1959. Miner started in the electronics industry with a number of designs in the "
																				+ "medical world, including a remote-control pacemaker. He moved to Atari in the late 1970s. One of "
																				+ "his first successes was to combine an entire breadboard of components into a single chip, known "
																				+ "as the TIA. The TIA was the display hardware for the Atari 2600, which would go on to sell millions."
																				+ " After working on the TIA he headed up the design of the follow-on chip set that would go on to"
																				+ " be the basis of the Atari 8-bit family of home computers, known as ANTIC and CTIA. "
																				+ "In the early 1980s Jay, along with other Atari staffers, had become fed up with management and "
																				+ "decamped. They set up another chipset project under a new company in Santa Clara, called "
																				+ "Hi-Toro (later renamed to Amiga Corporation), where they could have some creative freedom. "
																				+ "There, they started to create a new Motorola 68000-based games console, codenamed Lorraine, "
																				+ "that could be upgraded to a computer. To raise money for the Lorraine project, Amiga Corp."
																				+ " designed and sold joysticks and game cartridges for popular game consoles such as the"
																				+ " Atari 2600 and ColecoVision, as well as an odd input device called the Joyboard, essentially "
																				+ "a joystick the player stood on. Atari continued to be interested in the team's efforts throughout"
																				+ " this period, and funded them with $500,000 in capital in return for first use of their resulting"
																				+ " chipset. The Amiga crew, having continuing serious financial problems, had sought more monetary"
																				+ " support from investors that entire Spring. Amiga entered in to discussions with Commodore."
																				+ " The discussions ultimately led to Commodore wanting to purchase Amiga outright, which would"
																				+ " (from Commodore's viewpoint) cancel any outstanding contracts - including Atari Inc.'s."
																				+ " So instead of Amiga delivering the chipset, Commodore delivered a check of $500,000 to Atari"
																				+ " on Amiga's behalf, in effect returning the funds invested into Amiga for completion of the"
																				+ " Lorraine chipset. The original Amiga (1985) Jay worked at Commodore-Amiga for several years,"
																				+ " in Los Gatos (CA). They made good progress at the beginning, but as Commodore management"
																				+ " changed, they became marginalised and the original Amiga staff was fired or left out on a"
																				+ " one-by-one basis, until the entire Los Gatos office was closed. Miner later worked as a"
																				+ " consultant for Commodore until it went bankrupt. He was known as the 'Padre' (father) of"
																				+ " the Amiga among Amiga users. Jay always took his dog 'Mitchy' (a cockapoo) with him wherever"
																				+ " he went. While he worked at Atari, Mitchy even had her own ID-badge, and Mitchy's paw print"
																				+ " is visible on the inside of the Amiga 1000 top cover, alongside the signatures of the"
																				+ " engineers who worked on it. Jay endured kidney problems for most of his life, according"
																				+ " to his wife, and relied on dialysis. His sister donated one of her own. Miner died due"
																				+ " to complications from kidney failure at the age of 62, just two months after Commodore"
																				+ " declared bankruptcy.";
	private String[]						words;

	@Parameters(value = "url")
	public FullTextIndexTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
		words = TEXT.split(" ");
	}

	@Test
	public void testFullTextInsertion() {
		database.open("admin", "admin");

		ODocument doc = new ODocument(database);

		StringBuilder text = new StringBuilder();
		Random random = new Random(1000);

		for (int i = 0; i < TOT; ++i) {
			doc.reset();
			doc.setClassName("Whiz");
			doc.field("id", i);
			doc.field("date", new Date());

			text.setLength(0);
			for (int w = 0; w < 10; ++w) {
				if (w > 0)
					text.append(' ');
				text.append(words[random.nextInt(words.length - 1)]);
			}

			doc.field("text", text.toString());

			doc.save();
		}

		database.close();
	}

	@Test(dependsOnMethods = "testFullTextInsertion")
	public void testFullTextSearch() {
		database.open("admin", "admin");

		Set<ODocument> allDocs = new HashSet<ODocument>();

		for (int i = 0; i < words.length - 1; ++i) {
			List<ODocument> docs = database.query(new OSQLSynchQuery<Object>("SELECT FROM Whiz WHERE text containstext \"" + words[i]
					+ "\""));
			allDocs.addAll(docs);
		}

		Assert.assertEquals(allDocs.size(), TOT);

		database.close();
	}
}
