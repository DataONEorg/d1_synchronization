package org.dataone.cn.mercury;

import com.epublishing.schema.solr.Field;
import com.epublishing.schema.solr.Add.Doc;
import index_utils.DateHandler;
import index_utils.DocumentHandler;
import index_utils.DocumentHandlerException;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;


import java.io.IOException;
import java.io.InputStream;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import java.util.Locale;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpression;


import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;

public class XPath_Handler implements DocumentHandler {

        Logger logger = Logger.getLogger(this.getClass().getName());
        private LinkedHashMap<String, Map<String, String>> lookupProperties;
        private Solr_XPath_Prototype spe;

	private Doc doc;
	private static ArrayList<String> filterWords = new ArrayList<String>();

	public synchronized Doc getDocument2(InputStream is, Doc adoc)
			throws DocumentHandlerException {

		try {
			doc = adoc;

			// populateDocument2(is);
			populateDocument3(is);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DocumentHandlerException("Cannot parse XML document", e);
		}
		return doc;
	}

	public synchronized Doc getDocument(InputStream is, Doc adoc)
			throws DocumentHandlerException {

		try {
			doc = adoc;

			populateDocument3(is);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DocumentHandlerException("Cannot parse XML document", e);
		}
		return doc;
	}

	public DateHandler dh = new DateHandler();

	// ///////////////////////////////////////////////////////////////////////////////////////////////////////

	public enum Type {
		TEXT(0, "plain"), 
		CSV(1, "csv"), 
		GEO(2, "geo"), 
		DATE(3, "date"), 
		TEXT_SINGLE(4, "single valued field"),
		LOOKUP(5, "properties map lookup"),
		DEFAULTING_LOOKUP(6, "properties map lookup WITH A DEFAULT VALUE"),
		LIST(7, "select all from a list of path values"),
		DATELIST(8, "select one of list of possible path values, converting to date format"),
		CHOOSER(9,"select one of list of possible path values");

		private final int type;
		private final String description;

		Type(int aType, String desc) {
			this.type = aType;
			this.description = desc;
		}

		public int status() {
			return this.type;
		}

		public String description() {
			return this.description;
		}
	}

	private void populateDocument3(InputStream xmlFile)
			throws ParserConfigurationException, SAXException, IOException {
		/*
		 * Initialization must be done here for creating the Document, because
		 * the stream will be closed and throws an exception if we try to access
		 * it more than once.
		 */

		LinkedHashMap<String, XPathExpression> xp1 = new LinkedHashMap<String, XPathExpression>();

		DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
		domFactory.setNamespaceAware(true); // never forget this!
		DocumentBuilder builder = domFactory.newDocumentBuilder();
		Document doc3 = builder.parse(xmlFile);

                LinkedHashMap<String, LinkedHashMap<String, XPathExpression>> xpr_maps = spe.getXprMap();
		for (String str : xpr_maps.keySet()) {

			xp1 = xpr_maps.get(str);

			try {
				ArrayList<String> uniqueValueList = new ArrayList<String>();

				LinkedHashMap<String, ArrayList<String>> metadata = spe.getRawMetadata(doc3, xp1);
				Set<String> author_lnameList2 = new TreeSet<String>();
				String str2 ="";
				
				if(str.contains("_LIST")){
					str2 = "LIST";
				}else if(str.contains("_GEO")){
					str2 = "GEO";
				}else if(str.contains("_CSV")){
					str2 = "CSV";
				}else if(str.contains("_SINGLE")){
					str2 = "TEXT_SINGLE";
				}else if(str.endsWith("_DATE")){
					str2 = "DATE";
				}else if(str.contains("_DEFAULTING_LOOKUP")){
					str2 = "DEFAULTING_LOOKUP";
				}else if(str.contains("_DATELIST")){
					str2 = "DATELIST";
				}else if(str.endsWith("_TEXT")){
					str2 = "TEXT";
				}else if(str.contains("_LOOKUP")){
					str2 = "LOOKUP";
				}else if(str.contains("_CHOOSER")){
					str2 = "CHOOSER";
				}else{
					str2 = str;
				}
				Type typevar = Type.valueOf(str2);

				switch (typevar) {

				case TEXT_SINGLE:

					for (String txt : metadata.keySet()) {
						ArrayList<String> al = metadata.get(txt);
						if((null != txt)&&(txt.trim().length()>0)		
						){
							if (al.size() > 0) {
								String parameterValue = (((String) al.get(0)));
								parameterValue = parameterValue.replaceAll("&nbsp\\;", "").trim();
								if(parameterValue.length()>0){
									Field field = doc.addNewField();
									field.setName(txt);
									field.setStringValue(parameterValue);
								}
								
							}

						uniqueValueList.add(txt);
						}
					}

					break;

				case TEXT:

					for (String txt : metadata.keySet()) {
						if((null != txt)&&
								(txt.trim().length()>0)
								){
							ArrayList<String> al = metadata.get(txt);
							Set<String> distinctValues = new TreeSet<String>();
	
							for (int i = 0; i < al.size(); i++) {
								String parameterValue = (((String) al.get(i))
										.toLowerCase().replaceAll("&nbsp\\;", "").trim());
								distinctValues.add(parameterValue);
	
							}
							for (String var2 : distinctValues) {
								if (txt.equalsIgnoreCase("origin")) {
									String lname[] = var2.split("[,]");
									if (lname.length > 0) {
										author_lnameList2.add(lname[0]);
	
									}
								}
								Field field = doc.addNewField();
								field.setName(txt);
								field.setStringValue(var2);
							}
							uniqueValueList.add(txt);
						}
					}

					for (String author : author_lnameList2) {
						Field field = doc.addNewField();
						field.setName("author_lname");
						field.setStringValue(StringEscapeUtils
								.unescapeHtml(author));
					}
					break;

				case CSV:
					for (String csv : metadata.keySet()) {
						ArrayList<String> al = metadata.get(csv);
						Set<String> distinctValues = new TreeSet<String>();
						for (int i = 0; i < al.size(); i++) {
							String parameterValue = (((String) al.get(i))
									.toLowerCase());
							distinctValues.add(parameterValue);

						}
						for (String csv2 : distinctValues) {
							split_csv_and_add(csv, csv2);
						}
						uniqueValueList.add(csv);
					}

					break;

				case GEO:

					process_coords(metadata);

					break;

				case DATE:

					process_dates(metadata, uniqueValueList);

				
					break;
				case LOOKUP:
					
					process_lookups(metadata);
					
				
					break;
					
				case DATELIST:
					
					 process_date_array(metadata, uniqueValueList,"pubDate",1900);
					
				
					break;
					
				case LIST:
					
					 process_array(metadata);
					
				
					break;
					
				case CHOOSER:
					
					 process_array_choice(metadata,uniqueValueList);
					
				
					break;
			case DEFAULTING_LOOKUP:
				
				process_defaulting_lookups(metadata);
				
			}
				
			} catch (Exception exc) {
				logger.warn("Exception caught in xpath handler!! " + exc.getMessage());
				exc.printStackTrace();
			}

		}
/*
//		String fullText = spe.getFullText(doc3);
		String fullText = spe.getFullText2(xmlFile);
		Field field = doc.addNewField();
		field.setName("fullText");
		field.setStringValue(fullText);
		*/

	}
	private void process_defaulting_lookups(LinkedHashMap<String, ArrayList<String>> metadata){
		
		String cookedData="";
		// just check/init context for local testing. 
		// normal usage will have this passed in

		for (String solr_field : metadata.keySet()) {
			if((null != solr_field)&&(solr_field.trim().length()>0)){
				LinkedHashMap<String,String> hm = new LinkedHashMap<String,String>( (Map<String,String>)this.getLookupProperty(solr_field));
				ArrayList<String> al = metadata.get(solr_field);
				Set<String> lookups = new TreeSet<String>();
				
				for(String rawdata:al){
					cookedData = hm.get(rawdata.toLowerCase().trim());
					if ((null != cookedData)&&(cookedData.trim().length()>0)){
						lookups.add(cookedData);
					}else {
						lookups.add("others");
					}
				}
				
				for (String mapped_value : lookups) {
					Field field = doc.addNewField();
					field.setName(solr_field.trim());
					field.setStringValue(StringEscapeUtils
							.unescapeHtml(mapped_value));
				}
				
			}
		}
	}
	
	
	private void process_lookups(LinkedHashMap<String, ArrayList<String>> metadata){
		
		String cookedData="";
		// just check/init context for local testing. 
		// normal usage will have this passed in

		for (String solr_field : metadata.keySet()) {
			if((null != solr_field)&&(solr_field.trim().length()>0)){
				LinkedHashMap<String,String> hm = new LinkedHashMap<String,String>( (Map<String,String>)this.getLookupProperty(solr_field));
				ArrayList<String> al = metadata.get(solr_field);
				Set<String> lookups = new TreeSet<String>();
				
				for(String rawdata:al){
					cookedData = hm.get(rawdata.toLowerCase().trim());
					if ((null != cookedData)&&(cookedData.trim().length()>0)){
						lookups.add(cookedData);
					}else {
						lookups.add(rawdata);
					}
				}
				
				for (String mapped_value : lookups) {
					Field field = doc.addNewField();
					field.setName(solr_field.trim());
					field.setStringValue(StringEscapeUtils
							.unescapeHtml(mapped_value));
				}
				
			}
		}
	}

	private void process_dates(LinkedHashMap<String, ArrayList<String>> metadata,
			ArrayList<String> uniqueValueList) {

		for (String date : metadata.keySet()) {
			if((null !=date)&&(date.trim().length()>0)){
				ArrayList<String> al = metadata.get(date);
				if (al.size() > 0) {
					String parameterValue = (((String) al.get(0)));
					if (!(date.equalsIgnoreCase("begdate"))
							&& !(date.equalsIgnoreCase("enddate"))) {
						Field field = doc.addNewField();
						field.setName(date);
						field.setStringValue(dh.dateHandler(parameterValue).get(
								"date"));
						uniqueValueList.add(date);
					}
				}
			}
		}
		String edate = "";
		String bdate = "";
		if (metadata.keySet().contains("begdate")) {
			ArrayList al = metadata.get("begdate");
			for (int i = 0; i < al.size(); i++) {
				if (((String) al.get(i)).toLowerCase().trim().length() > 0) {
					bdate = (((String) al.get(i)).toLowerCase().trim());
				}
			}
		}

		if (metadata.keySet().contains("enddate")) {
			ArrayList al = metadata.get("enddate");
			for (int i = 0; i < al.size(); i++) {
				if (((String) al.get(i)).toLowerCase().trim().length() > 0) {
					edate = (((String) al.get(i)).toLowerCase().trim());
				}
			}
		}

		dh.doc_dates(uniqueValueList, edate, bdate, doc);

	}

	
	private void process_array_choice(LinkedHashMap<String, ArrayList<String>> metadata,ArrayList<String> uniqueValueList) {
		int iloop=0;
		String idata="";
		for (String data : metadata.keySet()) { // for each of the list of possible paths
			if(iloop ==0){// select the first named entry as the element name
				idata = data;
				iloop++;	
			}
			
			if((null !=data)&&(data.trim().length()>0)&&(!(uniqueValueList.contains(idata)))){
				ArrayList<String> al = metadata.get(data);
				
				if (al.size() > 0) {// only enter the loop if there exists metadata
					//this should handle any repeating elements by forcing use of first one
						String parameterValue = (((String) al.get(0)));
						Field field = doc.addNewField();
						field.setName(idata);
						field.setStringValue(parameterValue);
						uniqueValueList.add(idata); // this and the test should force a single selection by stopping the loop
					}
			}
		}
	}
	
	
	private void process_date_array(LinkedHashMap<String, ArrayList<String>> metadata,
			ArrayList<String> uniqueValueList, String index_name, int default_year) {
			
		for (String date : metadata.keySet()) { // for each of the list of possible paths
			if((null !=date)&&(date.trim().length()>0)&&(!(uniqueValueList.contains(index_name)))){
				ArrayList<String> al = metadata.get(date);
				if (al.size() > 0) {
					//this should handle any repeating elements by forcing use of first one
						String parameterValue = (((String) al.get(0)));
						Field field = doc.addNewField();
						field.setName(index_name);
						field.setStringValue(dh.dateHandler(parameterValue,default_year).get("date"));
						uniqueValueList.add(index_name);
					
				}
			}
		}
		if(!(uniqueValueList.contains(index_name))){
			
				String parameterValue = "nodate";
				Field field = doc.addNewField();
				field.setName(index_name);
				field.setStringValue(dh.dateHandler(parameterValue,default_year).get("date"));
				uniqueValueList.add(index_name);
			
		}
	}
	
	private void process_array(LinkedHashMap<String, ArrayList<String>> metadata) {
		// really a special case of TEXT, but the convention here is that the 
		// first "data" declares the name of the field being indexed 
		// and subsequent keys will only be used to extract values from the map
		int iloop=0;
		String idata="";
		for (String data : metadata.keySet()) { // for each of the list of possible paths
			if(iloop ==0){
				idata = data;
				iloop++;	
			}
			
			if((null !=data)&&(data.trim().length()>0)){
				ArrayList<String> al = metadata.get(data);
					for(String parameterValue:al){
						Field field = doc.addNewField();
						field.setName(idata);
						field.setStringValue(parameterValue);
					}
			}
		}
	}
	


	private void process_coords(LinkedHashMap<String, ArrayList<String>> metadata) {

		boolean bad_n = false, bad_s = false, bad_e = false, bad_w = false, nobox = false;
		boolean global = false;
		Field nobc = doc.addNewField();
		nobc.setName("noBoundingBox");

		float nbc = 5, sbc = 5, ebc = 5, wbc = 5;
		Field gfield = doc.addNewField();
		gfield.setName("isSpatial");

		if ((metadata.keySet().contains("westbc") && (metadata.keySet()
				.contains("eastbc")))
				&& (metadata.keySet().contains("northbc"))
				&& (metadata.keySet().contains("southbc"))) {

			String westBoundCoord = "", eastBoundCoord = "", northBoundCoord = "", southBoundCoord = "";

			if (metadata.keySet().contains("westbc")) {
				ArrayList al = metadata.get("westbc");
				for (int i = 0; i < al.size(); i++) {
					westBoundCoord = ((String) al.get(i));
				}
			}

			if (metadata.keySet().contains("eastbc")) {
				ArrayList al = metadata.get("eastbc");
				for (int i = 0; i < al.size(); i++) {
					eastBoundCoord = ((String) al.get(i));
				}
			}

			if (metadata.keySet().contains("northbc")) {
				ArrayList al = metadata.get("northbc");
				for (int i = 0; i < al.size(); i++) {
					northBoundCoord = ((String) al.get(i));
				}
			}

			if (metadata.keySet().contains("southbc")) {
				ArrayList al = metadata.get("southbc");
				for (int i = 0; i < al.size(); i++) {
					southBoundCoord = ((String) al.get(i));
				}
			}

			if (westBoundCoord != null && (westBoundCoord.length() > 0)) {

				try {
					wbc = Float.parseFloat(westBoundCoord);
				} catch (Exception ex) {
					bad_w = true;
				}

			}

			else {
				bad_w = true;
			}

			if (eastBoundCoord != null && (eastBoundCoord.length() > 0)) {

				try {
					ebc = Float.parseFloat(eastBoundCoord);

				} catch (Exception ex) {

					bad_e = true;
				}

			}

			else {
				bad_e = true;
			}

			if (northBoundCoord != null && (northBoundCoord.length() > 0)) {

				try {
					nbc = Float.parseFloat(northBoundCoord);
				} catch (Exception ex) {
					bad_n = true;
				}

			}

			else {
				bad_n = true;
			}

			if (southBoundCoord != null && (southBoundCoord.length() > 0)) {

				try {
					sbc = Float.parseFloat(southBoundCoord);
				} catch (Exception ex) {
					bad_s = true;
				}

			}

			else {
				bad_s = true;
			}
			// ///////////////
			if ((bad_n && bad_s && bad_e && bad_w) || nobox) {

				nobc.setStringValue("true");
				gfield.setStringValue("false");

			} else {
				nobc.setStringValue("false");
			}
			try {
				if ((ebc == 180 && wbc == -180)
						&& ((sbc == -90 && nbc == 90) || (sbc == -81 && nbc == 81))) {
					global = true;
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			if (global) {
				gfield.setStringValue("true");
			} else {

				gfield.setStringValue("false");
			}

			// //////////////////
			if (bad_n && bad_s && bad_e && bad_w) {

				// Field nobc2 = doc.addNewField();
				// nobc2.setName("noBoundingBox");
				nobc.setStringValue("true");
				gfield.setStringValue("false");
			} else {

				if (bad_n) {
					if (!bad_s) {
						Field field = doc.addNewField();
						field.setName("northBoundCoord");
						field.setStringValue(southBoundCoord);
					} else {
						Field field = doc.addNewField();
						field.setName("northBoundCoord");
						field.setStringValue("90.0");
					}
				} else {
					Field field = doc.addNewField();
					field.setName("northBoundCoord");
					field.setStringValue(northBoundCoord);
				}

				if (bad_s) {
					if (!bad_n) {
						Field field = doc.addNewField();
						field.setName("southBoundCoord");
						field.setStringValue(northBoundCoord);
					} else {
						Field field = doc.addNewField();
						field.setName("southBoundCoord");
						field.setStringValue("-90.0");
					}
				} else {
					Field field = doc.addNewField();
					field.setName("southBoundCoord");
					field.setStringValue(southBoundCoord);
				}

				if (bad_e) {
					if (!bad_w) {
						Field field = doc.addNewField();
						field.setName("eastBoundCoord");
						field.setStringValue(westBoundCoord);
					} else {
						Field field = doc.addNewField();
						field.setName("eastBoundCoord");
						field.setStringValue("180.0");
					}
				} else {
					Field field = doc.addNewField();
					field.setName("eastBoundCoord");
					field.setStringValue(eastBoundCoord);
				}

				if (bad_w) {
					if (!bad_e) {
						// set west
						Field field = doc.addNewField();
						field.setName("westBoundCoord");
						field.setStringValue(eastBoundCoord);
					} else {
						Field field = doc.addNewField();
						field.setName("westBoundCoord");
						field.setStringValue("-180.0");
					}

				} else {

					Field field = doc.addNewField();
					field.setName("westBoundCoord");
					field.setStringValue(westBoundCoord);
				}
			}
		} else {

			nobox = true;

			nobc.setStringValue("true");
			gfield.setStringValue("false");
		}

	}

	private boolean split_csv_and_add(String field_name, String csv_string) {
		if((null !=field_name)&&(field_name.trim().length()>0)){
			String[] data_array = csv_string.split("[,;|>:]");
			for (int j = 0; j < data_array.length; j++) {
				Field field = doc.addNewField();
				field.setName(field_name);
				field.setStringValue(StringEscapeUtils.unescapeHtml(data_array[j]
						.toLowerCase()));
			}
		}
		return true;
	}

	private boolean isGarbage(String inputStr) {
		filterWords.add("n/a");
		filterWords.add("unknown");
		filterWords.add("none");
		filterWords.add("present");
		filterWords.add("varies");
		filterWords.add("variable");
		filterWords.add("unlnown");
		filterWords.add("unlnownt");
		filterWords.add("unkn");
		filterWords.add("&nbsp;&nbsp;");

		// add more here

		Iterator<String> iter = filterWords.iterator();
		while (iter.hasNext()) {
			String filterWord = iter.next();
			if (inputStr.equalsIgnoreCase(filterWord)) {
				return true;
			}
		}
		// no filter word found so it is valid
		return false;
	}

	public SimpleDateFormat Solr_ISO8601FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ssZ");
	// cdate =
	// yada_Solr_Solr_ISO8601FORMAT.format(DATE_8.getCalendar().getTime());
	public SimpleDateFormat Solr_RFC822DATEFORMAT = new SimpleDateFormat(
			"EEE', 'dd' 'MMM' 'yyyy' 'HH:mm:ss' 'Z", Locale.US);

	public SimpleDateFormat yada_Solr_Solr_ISO8601FORMAT = new SimpleDateFormat(
			"yyyy'-'MM'-'dd'T'HH:mm:ss'Z'");

	public String convertToRFC822String(String date) {
		try {
			Solr_ISO8601FORMAT.parse(date, new ParsePosition(0));
		} catch (Exception pex) {
			pex.printStackTrace();
		}
		return Solr_RFC822DATEFORMAT.format(Solr_ISO8601FORMAT.getCalendar()
				.getTime());
	}

	public String convertToISO8601FORMAT(String date) {
		try {
			Solr_ISO8601FORMAT.parse(date, new ParsePosition(0));
		} catch (Exception pex) {
			pex.printStackTrace();
		}
		return yada_Solr_Solr_ISO8601FORMAT.format(Solr_ISO8601FORMAT
				.getCalendar().getTime());
	}

	public SimpleDateFormat Modis_Solr_ISO8601FORMAT = new SimpleDateFormat(
			"yyyyMMdd");

	public String Modis_convertToISO8601FORMAT(String date) {
		try {
			Modis_Solr_ISO8601FORMAT.parse(date, new ParsePosition(0));
		} catch (Exception pex) {
			pex.printStackTrace();
		}
		return yada_Solr_Solr_ISO8601FORMAT.format(Modis_Solr_ISO8601FORMAT
				.getCalendar().getTime());
	}
    public Map<String, String> getLookupProperty(String propertyName) {
        return lookupProperties.get(propertyName);
    }

    public LinkedHashMap<String, Map<String, String>> getLookupProperties() {
        return lookupProperties;
    }

    public void setLookupProperties(LinkedHashMap<String, Map<String, String>> lookupProperties) {
        this.lookupProperties = lookupProperties;
    }

    public Solr_XPath_Prototype getSpe() {
        return spe;
    }

    public void setSpe(org.dataone.cn.mercury.Solr_XPath_Prototype spe) {
        this.spe = spe;
    }


}
