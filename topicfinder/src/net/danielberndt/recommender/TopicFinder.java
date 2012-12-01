package net.danielberndt.recommender;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import cc.mallet.topics.ParallelTopicModel;
import cc.mallet.types.Alphabet;
import cc.mallet.types.FeatureSequence;
import cc.mallet.types.IDSorter;
import cc.mallet.types.Instance;
import cc.mallet.types.InstanceList;

import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class TopicFinder {

	private static final int LOWERTHRESHOLD = 2;
	private static final String dbName = "lastfm_lda";
	private static final String inCollName = "user_top_albums";
	private static final String topicDistCollName = "topic_dist_albums";
	private static final String topicCollName = "topics";
	
	private static DBCollection getCollection(String name) throws Exception {
		Mongo m = new Mongo();
		DB db = m.getDB(dbName);
		return db.getCollection(name);
	}
	
	private static Map<AlbumToken, DBObject> albumToDBObj = Maps.newHashMapWithExpectedSize(50000);
	
	public static Multimap<String, AlbumToken> getData() throws Exception  {
		Multimap<String, AlbumToken> userToAlbums = HashMultimap.create();
		DBCursor cursor = getCollection(inCollName).find(new BasicDBObject("period", "12month"), BasicDBObjectBuilder.start().add("name", true).add("top_albums", true).get());
		for (DBObject obj : cursor) {
			String userName = (String)obj.get("name");
			for (Object albumObj : ((BasicDBList)obj.get("top_albums"))) {
				DBObject album = (DBObject)albumObj;
				AlbumToken at =  new AlbumToken((String)album.get("artist"), (String)album.get("name"));
				if (!albumToDBObj.containsKey(album)) {
					album.removeField("playcount");
					albumToDBObj.put(at, album);
				}
				userToAlbums.put(userName, at);
			}
		}
		return userToAlbums;
	}
	
	public static void main(String[] args) throws Exception {
		Multimap<String, AlbumToken> data = getData();
		Alphabet dataAlphabet = new Alphabet();
		
		Map<String, FeatureSequence> features = Maps.newHashMapWithExpectedSize(data.size()/2);
		for (Map.Entry<String, Collection<AlbumToken>> e: data.asMap().entrySet()) {
			FeatureSequence seq = new FeatureSequence(dataAlphabet);
			int min = Integer.MAX_VALUE;
			for (AlbumToken album: e.getValue()) if (album.getCount()<min) min=album.getCount();
			for (AlbumToken album: e.getValue()) {
				if (album.getCount()>LOWERTHRESHOLD) {
					for (int i=0,l=(int)Math.pow(album.getCount()/min, 0.5);i<l;i++) seq.add(album);
				}
			}
			features.put(e.getKey(), seq);
		}
		
		InstanceList instances = new InstanceList(dataAlphabet, null);
		for (Map.Entry<String, FeatureSequence> e: features.entrySet()) {
			instances.add(new Instance(e.getValue(), null, e.getKey(), null));
		}
		int numTopics = 150;
		ParallelTopicModel model = new ParallelTopicModel(numTopics);
		model.addInstances(instances);
		model.setNumThreads(1);
        model.setNumIterations(300);
        model.estimate();
        
        entryProbsToDB(getTopicDistributionOverTypes(model),model.getAlphabet());
        enterBestTopicMatchToDB(model.getSortedWords(), model.getAlphabet());

	}

	private static Map<Integer, TopicDistribution> getTopicDistributionOverTypes(ParallelTopicModel model) {
		Map<Integer, TopicDistribution> map = Maps.newHashMapWithExpectedSize(model.getAlphabet().size());
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
			map.put(type, new TopicDistribution(dist, ((AlbumToken)model.getAlphabet().lookupObject(type)).getCount()));
		}
		return map;
	}
	
	private static void entryProbsToDB(Map<Integer, TopicDistribution> dists, Alphabet alphabet) throws Exception {
		DBCollection col = getCollection(topicDistCollName);
		col.drop();
		
		int interval = 1000;
		List<DBObject> toBeSaved = Lists.newArrayListWithCapacity(interval);
		for (Map.Entry<Integer, TopicDistribution> e: dists.entrySet()) {
			AlbumToken album = (AlbumToken)alphabet.lookupObject(e.getKey());
			DBObject saveObj = BasicDBObjectBuilder.start("count", e.getValue().count).add("distribution", e.getValue().distribution).get();
			saveObj.putAll(albumToDBObj.get(album));
			toBeSaved.add(saveObj);
			if (toBeSaved.size() == interval) {
				col.insert(toBeSaved);
				toBeSaved.clear();
			}
		}
		col.insert(toBeSaved);
		col.ensureIndex(BasicDBObjectBuilder.start("artist", 1).add("name", 1).get());
		System.out.println(String.format("WROTE %s TO DB", col.getName()));
	}
	
	private static void enterBestTopicMatchToDB(List<TreeSet<IDSorter>> topicSortedWords, Alphabet alphabet) throws Exception {
		DBCollection col = getCollection(topicCollName);
		col.drop();
		
		List<DBObject> toBeSaved = Lists.newArrayListWithCapacity(topicSortedWords.size()*topicSortedWords.get(0).size());
		int topic=0;
		for (Set<IDSorter> albums: topicSortedWords) {
			for (IDSorter info: albums) {
				AlbumToken album = (AlbumToken)alphabet.lookupObject(info.getID());
				DBObject saveObj = BasicDBObjectBuilder.start("topic",topic).add("count", info.getWeight()).add("totalCount", album.getCount()).get();
				saveObj.putAll(albumToDBObj.get(album));
				toBeSaved.add(saveObj);
			}
			topic ++;
		}
		
		col.insert(toBeSaved);
		col.ensureIndex(BasicDBObjectBuilder.start("topic", 1).add("count", -1).get());
		System.out.println(String.format("WROTE %s TO DB", col.getName()));
	}
	
	public static class AlbumToken {
		public final String artist;
		public final String album;
		
		private static Multiset<AlbumToken> counter = HashMultiset.create();
		
		public AlbumToken(String artist, String album) {
			this.artist = artist;
			this.album = album;
			counter.add(this);
		}
		
		public int getCount() {
			return counter.count(this);
		}
		
		@Override
		public boolean equals(Object o) {
			if (!(o instanceof AlbumToken)) return false;
			AlbumToken other = (AlbumToken)o;
			return this.album.equals(other.album) && this.artist.equals(other.artist);
		}
		
		@Override
		public int hashCode() {
			return Objects.hashCode(artist, album);
		}
		
		@Override
		public String toString() {
			return String.format("%s – '%s'", artist, album);
		}
	} 
	
	public static class TopicDistribution {
		public final double[] distribution;
		public final int count;
		
		public TopicDistribution(double[] distribution, int count) {
			this.distribution = distribution;
			this.count = count;
		}
	}
	
}
