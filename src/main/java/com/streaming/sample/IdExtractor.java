package com.streaming.sample;

import java.io.Serializable;
import java.io.StringReader;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.spark.sql.api.java.UDF1;

public class IdExtractor implements UDF1<String, String>, Serializable {

	@Override
	public String call(String txnXml) throws Exception {
		JAXBContext ctx = JAXBContext.newInstance(Transaction.class);
		Unmarshaller unmarshaller = ctx.createUnmarshaller();
		Transaction txn = (Transaction) unmarshaller.unmarshal(new StringReader(txnXml));
		return txn.getId();
	}
}
