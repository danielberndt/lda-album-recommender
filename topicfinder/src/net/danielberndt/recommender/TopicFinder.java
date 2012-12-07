package net.danielberndt.recommender;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cc.mallet.topics.ParallelTopicModel;
import cc.mallet.types.Alphabet;
import cc.mallet.types.FeatureSequence;
import cc.mallet.types.Instance;
import cc.mallet.types.InstanceList;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;



public class TopicFinder {
	
	private static final String dbName = "lastfm_lda";
	private static final String inCollName = "user_top_albums";
	private static final String albumsWithTopicsName = "albums";
	
	private static Map<String, String> albumTransformer;
	private static Map<String, Integer> albumCount;
	private static DBCursor cursor = getCollection(inCollName).find(new BasicDBObject("period", "12month"), BasicDBObjectBuilder.start().add("name", true).add("top_albums", true).get());

	private static DBCollection getCollection(String name) {
		try {
			Mongo m = new Mongo();
			DB db = m.getDB(dbName);
			return db.getCollection(name);
		} 
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private static Map<String, List<String>> getData() {
		Map<String, List<String>> userToAlbums = Maps.newHashMapWithExpectedSize(5000);
		Map<String, Multiset<String>> artistToAlbums = Maps.newHashMapWithExpectedSize(1000);
		
		int i=0;
		
		for (DBObject obj : cursor) {
			i++;
			if (i%1000==0) System.out.println("read user "+i);
			String userName = (String)obj.get("name");
			for (Object albumObj : ((BasicDBList)obj.get("top_albums"))) {
				DBObject albumDBObj = (DBObject)albumObj;
				String artist = (String)albumDBObj.get("artist");
				String album = (String)albumDBObj.get("name");
				Multiset<String> albums = artistToAlbums.get(artist);
				if (albums==null) {
					albums = HashMultiset.create();
					artistToAlbums.put(artist, albums);
				}
				albums.add(album);
				String albumKey = artist+'§'+album;
				List<String> list = userToAlbums.get(userName);
				if (list==null) {
					list = new ArrayList<String>(70);
					userToAlbums.put(userName, list);
				}
				list.add(albumKey);
			}
		}
		albumTransformer = clusterAlbums(artistToAlbums);
		for (Map.Entry<String, List<String>> e: userToAlbums.entrySet()) {
			List<String> newList = Lists.newArrayListWithCapacity(e.getValue().size());
			for (String albumKey: e.getValue()) newList.add(albumTransformer.get(albumKey));
		}
		
		return userToAlbums;
	}
	
	private static Map<String, String> clusterAlbums(Map<String, Multiset<String>> artistToAlbums) {
		Map<String, String> map = Maps.newHashMapWithExpectedSize(artistToAlbums.size()*3);
		albumCount = Maps.newHashMapWithExpectedSize(artistToAlbums.size()*3);
		for (Map.Entry<String, Multiset<String>> e: artistToAlbums.entrySet()) {
			for (Multiset.Entry<String> albumEntry: e.getValue().entrySet()) {
				String source = e.getKey()+'§'+albumEntry.getElement();
				map.put(source, source);
				albumCount.put(source, albumEntry.getCount());
			}
		}
		return map;
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("start");
		Map<String, List<String>> data = getData();
		Alphabet dataAlphabet = new Alphabet();
		
		Map<String, FeatureSequence> features = Maps.newHashMapWithExpectedSize(data.size());
		for (Map.Entry<String, List<String>> e: data.entrySet()) {
			FeatureSequence seq = new FeatureSequence(dataAlphabet);
			for (String albumKey: e.getValue()) seq.add(albumKey);
			features.put(e.getKey(), seq);
		}
		
		InstanceList instances = new InstanceList(dataAlphabet, null);
		for (Map.Entry<String, FeatureSequence> e: features.entrySet()) {
			instances.add(new Instance(e.getValue(), null, e.getKey(), null));
		}
		int numTopics = 150;
		ParallelTopicModel model = new ParallelTopicModel(numTopics);
		model.addInstances(instances);
        model.setNumIterations(150);
        model.estimate();
        entryProbsToDB(getTopicDistributionOverTypes(model),model.getAlphabet());
	}
	
	private static Map<Integer, double[]> getTopicDistributionOverTypes(ParallelTopicModel model) {
		Map<Integer, double[]> map = Maps.newHashMapWithExpectedSize(model.getAlphabet().size());
		for (int type=0; type<model.getAlphabet().size(); type++) {
			double sum = 0;
			double[] dist = new double[model.getNumTopics()];
			for (int topic: model.typeTopicCounts[type]) {
				if (topic==0) break;
				double weight = model.beta + (topic >> model.topicBits);
				dist[topic & model.topicMask] = weight;
				sum+=weight;
			}
			for (int i=0;i<model.getNumTopics();i++) dist[i]/=sum; // normalise
			map.put(type, dist);
		}
		return map;
	}
	
	private static void entryProbsToDB(Map<Integer, double[]> dists, Alphabet alphabet) throws Exception {
		DBCollection col = getCollection(albumsWithTopicsName);
		col.drop();
		
		int interval = 1000;
		List<DBObject> toBeSaved = Lists.newArrayListWithCapacity(interval+interval/5);
		Set<String> saved = Sets.newHashSetWithExpectedSize(dists.size());
		for (DBObject obj : cursor) {
			for (Object albumObj : ((BasicDBList)obj.get("top_albums"))) {
				DBObject albumDBObj = (DBObject)albumObj;
				String artist = (String)albumDBObj.get("artist");
				String album = (String)albumDBObj.get("name");
				String albumKey = artist+'§'+album;
				
				if (!saved.add(albumKey)) continue;
				
				double[] dist = dists.get(alphabet.lookupIndex(albumKey));
				int count = albumCount.get(albumKey);
				
				albumDBObj.removeField("playcount");
				albumDBObj.put("count", count);
				albumDBObj.put("distribution", dist);
				
				List<DBObject> topics = Lists.newArrayList();
				for (int topic = 0; topic < dist.length; topic++) {
					double d = dist[topic];
					if (d==0) continue;
					topics.add(BasicDBObjectBuilder.start("topic",topic).add("count", d*count).get());
				}
				albumDBObj.put("topics", topics);
				
				toBeSaved.add(albumDBObj);
			}
			
			if (toBeSaved.size() >= interval) {
				System.out.println(String.format("saved: %.2f%%", (saved.size()/(double)dists.size())*100));
				col.insert(toBeSaved);
				toBeSaved.clear();
			}
		}
		
		col.insert(toBeSaved);
		col.ensureIndex(BasicDBObjectBuilder.start("artist", 1).add("name", 1).get());
		col.ensureIndex(BasicDBObjectBuilder.start("topics.topic", 1).add("topics.count", -1).get());
	}
	
}
