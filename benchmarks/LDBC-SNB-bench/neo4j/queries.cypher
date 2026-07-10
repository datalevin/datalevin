:param personId => 100;
:param firstName => "John";
:param maxDate => "2012-07-01T00:00:00Z";
:param countryXName => "Germany";
:param countryYName => "France";
:param countryName => "Germany";
:param startDate => "2011-01-01T00:00:00Z";
:param durationDays => 365;
:param minDate => "2011-01-01T00:00:00Z";
:param tagName => "Mozart";
:param month => 5;
:param workFromYear => 2010;
:param tagClassName => "MusicalArtist";
:param person1Id => 100;
:param person2Id => 6597069770569;
:param messageId => 1099512606636;

// IC1
MATCH (start:Person {id: $personId})
MATCH path = (start)-[:KNOWS*1..3]-(other:Person {firstName: $firstName})
WHERE other <> start
WITH other, min(length(path)) AS distance
MATCH (other)-[:IS_LOCATED_IN]->(city:Place)
OPTIONAL MATCH (other)-[study:STUDY_AT]->(uni:Organisation)-[:IS_LOCATED_IN]->(uniCity:Place)
WITH other, distance, city,
     [u IN collect(DISTINCT [uni.name, study.classYear, uniCity.name]) WHERE u[0] IS NOT NULL] AS universities
OPTIONAL MATCH (other)-[work:WORK_AT]->(company:Organisation)-[:IS_LOCATED_IN]->(companyPlace:Place)
OPTIONAL MATCH (companyPlace)-[:IS_PART_OF*0..]->(companyCountry:Place {type: "Country"})
WITH other, distance, city, universities,
     [c IN collect(DISTINCT [company.name, work.workFrom, companyCountry.name]) WHERE c[0] IS NOT NULL] AS companies
RETURN "IC1" AS query,
       other.id,
       other.lastName,
       distance,
       other.birthday,
       other.creationDate,
       other.gender,
       other.browserUsed,
       other.locationIP,
       other.email,
       other.speaks,
       city.name,
       universities,
       companies
ORDER BY distance ASC, other.lastName ASC, other.id ASC
LIMIT 20;

// IC2
MATCH (start:Person {id: $personId})-[:KNOWS]-(friend:Person)
WITH DISTINCT friend, datetime($maxDate) AS maxDate
CALL {
  WITH friend, maxDate
  MATCH (friend)<-[:HAS_CREATOR]-(message:Post)
  WHERE message.creationDate < maxDate
  RETURN message
  UNION ALL
  WITH friend, maxDate
  MATCH (friend)<-[:HAS_CREATOR]-(message:Comment)
  WHERE message.creationDate < maxDate
  RETURN message
}
RETURN "IC2" AS query,
       friend.id,
       friend.firstName,
       friend.lastName,
       message.id,
       coalesce(message.content, message.imageFile) AS content,
       message.creationDate
ORDER BY message.creationDate DESC, message.id ASC
LIMIT 20;

// IC3
MATCH (start:Person {id: $personId})
MATCH (start)-[:KNOWS*1..2]-(other:Person)
WHERE other <> start
WITH DISTINCT other, datetime($startDate) AS startDate, duration({days: $durationDays}) AS window
MATCH (other)-[:IS_LOCATED_IN]->(city:Place)-[:IS_PART_OF*0..]->(home:Place {type: "Country"})
WHERE home.name <> $countryXName AND home.name <> $countryYName
WITH other, startDate, startDate + window AS endDate
MATCH (other)<-[:HAS_CREATOR]-(message)-[:IS_LOCATED_IN]->(country:Place)
WHERE (message:Post OR message:Comment)
  AND message.creationDate >= startDate
  AND message.creationDate < endDate
  AND country.name IN [$countryXName, $countryYName]
WITH other, country.name AS countryName, count(*) AS messageCount
WITH other,
     sum(CASE WHEN countryName = $countryXName THEN messageCount ELSE 0 END) AS xCount,
     sum(CASE WHEN countryName = $countryYName THEN messageCount ELSE 0 END) AS yCount
WHERE xCount > 0 AND yCount > 0
RETURN "IC3" AS query,
       other.id,
       other.firstName,
       other.lastName,
       xCount,
       yCount,
       xCount + yCount AS count
ORDER BY count DESC, other.id ASC
LIMIT 20;

// IC4
MATCH (start:Person {id: $personId})-[:KNOWS]-(friend:Person)
WITH collect(DISTINCT friend) AS friends,
     datetime($startDate) AS startDate,
     duration({days: $durationDays}) AS window
WITH friends, startDate, startDate + window AS endDate
CALL {
  WITH friends, startDate
  MATCH (f)<-[:HAS_CREATOR]-(oldPost:Post)-[:HAS_TAG]->(oldTag:Tag)
  WHERE f IN friends AND oldPost.creationDate < startDate
  RETURN collect(DISTINCT oldTag) AS oldTags
}
CALL {
  WITH friends, startDate, endDate
  MATCH (f)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag)
  WHERE f IN friends AND post.creationDate >= startDate AND post.creationDate < endDate
  RETURN tag, count(DISTINCT post) AS postCount
}
WITH tag, postCount, oldTags
WHERE NOT tag IN oldTags
RETURN "IC4" AS query, tag.name, postCount
ORDER BY postCount DESC, tag.name ASC
LIMIT 10;

// IC5
MATCH (start:Person {id: $personId})
MATCH (start)-[:KNOWS*1..2]-(other:Person)
WHERE other <> start
WITH DISTINCT other, datetime($minDate) AS minDate
MATCH (forum:Forum)-[member:HAS_MEMBER]->(other)
WHERE member.joinDate > minDate
WITH forum, collect(DISTINCT other) AS members
OPTIONAL MATCH (forum)<-[:IS_CONTAINED_IN]-(post:Post)-[:HAS_CREATOR]->(memberPerson:Person)
WHERE memberPerson IN members
WITH forum, count(DISTINCT post) AS postCount
RETURN "IC5" AS query, forum.title, postCount
ORDER BY postCount DESC, forum.id ASC
LIMIT 20;

// IC6
MATCH (start:Person {id: $personId})
MATCH (start)-[:KNOWS*1..2]-(other:Person)
WHERE other <> start
WITH DISTINCT other
MATCH (other)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag {name: $tagName})
MATCH (post)-[:HAS_TAG]->(otherTag:Tag)
WHERE otherTag <> tag
WITH otherTag, count(DISTINCT post) AS postCount
RETURN "IC6" AS query, otherTag.name, postCount
ORDER BY postCount DESC, otherTag.name ASC
LIMIT 10;

// IC7
MATCH (start:Person {id: $personId})
CALL {
  WITH start
  MATCH (start)<-[:HAS_CREATOR]-(post:Post)<-[like:LIKES_POST]-(liker:Person)
  RETURN liker, like.creationDate AS likeDate, post AS message, post.creationDate AS messageDate,
         coalesce(post.content, post.imageFile) AS messageContent
  UNION ALL
  WITH start
  MATCH (start)<-[:HAS_CREATOR]-(comment:Comment)<-[like:LIKES_COMMENT]-(liker:Person)
  RETURN liker, like.creationDate AS likeDate, comment AS message, comment.creationDate AS messageDate,
         comment.content AS messageContent
}
WITH start, liker, likeDate, message, messageDate, messageContent
ORDER BY liker.id ASC, likeDate DESC, message.id ASC
WITH start, liker,
     collect({likeDate: likeDate, message: message, messageDate: messageDate, messageContent: messageContent}) AS likes
WITH start, liker, likes[0] AS latest
RETURN "IC7" AS query,
       liker.id,
       liker.firstName,
       liker.lastName,
       latest.likeDate,
       latest.message.id,
       latest.messageContent,
       toInteger((latest.likeDate.epochMillis - latest.messageDate.epochMillis) / 60000) AS minutesLatency,
       CASE
         WHEN liker = start THEN false
         WHEN (start)-[:KNOWS]-(liker) THEN false
         ELSE true
       END AS isNew
ORDER BY latest.likeDate DESC, liker.id ASC
LIMIT 20;

// IC8
MATCH (start:Person {id: $personId})
CALL {
  WITH start
  MATCH (start)<-[:HAS_CREATOR]-(post:Post)<-[:REPLY_OF_POST]-(comment:Comment)
  RETURN comment
  UNION ALL
  WITH start
  MATCH (start)<-[:HAS_CREATOR]-(parent:Comment)<-[:REPLY_OF_COMMENT]-(comment:Comment)
  RETURN comment
}
MATCH (comment)-[:HAS_CREATOR]->(author:Person)
RETURN "IC8" AS query,
       author.id,
       author.firstName,
       author.lastName,
       comment.creationDate,
       comment.id,
       comment.content
ORDER BY comment.creationDate DESC, comment.id ASC
LIMIT 20;

// IC9
MATCH (start:Person {id: $personId})
MATCH (start)-[:KNOWS*1..2]-(other:Person)
WHERE other <> start
WITH DISTINCT other, datetime($maxDate) AS maxDate
CALL {
  WITH other, maxDate
  MATCH (other)<-[:HAS_CREATOR]-(message:Post)
  WHERE message.creationDate < maxDate
  RETURN message
  UNION ALL
  WITH other, maxDate
  MATCH (other)<-[:HAS_CREATOR]-(message:Comment)
  WHERE message.creationDate < maxDate
  RETURN message
}
RETURN "IC9" AS query,
       other.id,
       other.firstName,
       other.lastName,
       message.id,
       coalesce(message.content, message.imageFile) AS content,
       message.creationDate
ORDER BY message.creationDate DESC, message.id ASC
LIMIT 20;

// IC10
MATCH (start:Person {id: $personId})-[:KNOWS]-(friend:Person)
WITH start, collect(DISTINCT friend) AS friends,
     CASE WHEN $month = 12 THEN 1 ELSE $month + 1 END AS nextMonth
MATCH (start)-[:KNOWS*2]-(foaf:Person)
WHERE foaf <> start
  AND NOT foaf IN friends
  AND foaf.birthday IS NOT NULL
  AND ((foaf.birthday.month = $month AND foaf.birthday.day >= 21)
       OR (foaf.birthday.month = nextMonth AND foaf.birthday.day < 22))
WITH DISTINCT start, foaf
OPTIONAL MATCH (foaf)<-[:HAS_CREATOR]-(post:Post)
WITH start, foaf, collect(DISTINCT post) AS posts
OPTIONAL MATCH (foaf)<-[:HAS_CREATOR]-(post2:Post)-[:HAS_TAG]->(:Tag)<-[:HAS_INTEREST]-(start)
WITH foaf, posts, collect(DISTINCT post2) AS commonPosts
WITH foaf,
     size(posts) AS total,
     size(commonPosts) AS common
WITH foaf, (common - (total - common)) AS commonInterestScore
MATCH (foaf)-[:IS_LOCATED_IN]->(city:Place)
RETURN "IC10" AS query,
       foaf.id,
       foaf.firstName,
       foaf.lastName,
       commonInterestScore,
       foaf.gender,
       city.name
ORDER BY commonInterestScore DESC, foaf.id ASC
LIMIT 10;

// IC11
MATCH (start:Person {id: $personId})
MATCH (start)-[:KNOWS*1..2]-(other:Person)
WHERE other <> start
WITH DISTINCT other
MATCH (other)-[work:WORK_AT]->(company:Organisation)-[:IS_LOCATED_IN]->(place:Place)
MATCH (place)-[:IS_PART_OF*0..]->(country:Place {type: "Country"})
WHERE country.name = $countryName
  AND work.workFrom < $workFromYear
RETURN "IC11" AS query,
       other.id,
       other.firstName,
       other.lastName,
       company.name,
       work.workFrom
ORDER BY work.workFrom ASC, other.id ASC, company.name DESC
LIMIT 10;

// IC12
MATCH (start:Person {id: $personId})-[:KNOWS]-(friend:Person)
MATCH (root:TagClass {name: $tagClassName})
MATCH (friend)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF_POST]->(post:Post)-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tc:TagClass)
WHERE (tc)-[:IS_SUBCLASS_OF*0..]->(root)
WITH friend,
     collect(DISTINCT comment) AS comments,
     collect(DISTINCT tag.name) AS tagNames
WHERE size(comments) > 0
RETURN "IC12" AS query,
       friend.id,
       friend.firstName,
       friend.lastName,
       tagNames,
       size(comments) AS replyCount
ORDER BY replyCount DESC, friend.id ASC
LIMIT 20;

// IC13
MATCH (p1:Person {id: $person1Id}), (p2:Person {id: $person2Id})
CALL {
  WITH p1, p2
  OPTIONAL MATCH path = shortestPath((p1)-[:KNOWS*1..]-(p2))
  RETURN length(path) AS distance
}
RETURN "IC13" AS query,
       CASE
         WHEN p1 = p2 THEN 0
         WHEN distance IS NULL THEN -1
         ELSE distance
       END AS shortestPathLength;

// IC14 (v1)
MATCH (p1:Person {id: $person1Id}), (p2:Person {id: $person2Id})
MATCH path = allShortestPaths((p1)-[:KNOWS*1..]-(p2))
WITH path, nodes(path) AS persons
WITH path, persons, range(0, size(persons) - 2) AS idxs
UNWIND idxs AS i
WITH path, persons[i] AS a, persons[i + 1] AS b
CALL {
  WITH a, b
  MATCH (a)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF_POST]->(:Post)-[:HAS_CREATOR]->(b)
  RETURN count(c) AS postRepliesAB
}
CALL {
  WITH a, b
  MATCH (a)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF_COMMENT]->(:Comment)-[:HAS_CREATOR]->(b)
  RETURN count(c) AS commentRepliesAB
}
CALL {
  WITH a, b
  MATCH (b)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF_POST]->(:Post)-[:HAS_CREATOR]->(a)
  RETURN count(c) AS postRepliesBA
}
CALL {
  WITH a, b
  MATCH (b)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF_COMMENT]->(:Comment)-[:HAS_CREATOR]->(a)
  RETURN count(c) AS commentRepliesBA
}
WITH path,
     (postRepliesAB + postRepliesBA) * 1.0 + (commentRepliesAB + commentRepliesBA) * 0.5 AS pairWeight
WITH path, collect(pairWeight) AS pairWeights
RETURN "IC14" AS query,
       [p IN nodes(path) | p.id] AS personIdsInPath,
       reduce(total = 0.0, w IN pairWeights | total + w) AS pathWeight
ORDER BY pathWeight DESC;

// IS1
MATCH (p:Person {id: $personId})-[:IS_LOCATED_IN]->(city:Place)
RETURN "IS1" AS query,
       p.firstName,
       p.lastName,
       p.birthday,
       p.locationIP,
       p.browserUsed,
       city.id,
       p.gender,
       p.creationDate;

// IS2
MATCH (p:Person {id: $personId})
CALL {
  WITH p
  MATCH (p)<-[:HAS_CREATOR]-(message:Post)
  RETURN message, message AS post, p AS originalPoster
  UNION ALL
  WITH p
  MATCH (p)<-[:HAS_CREATOR]-(message:Comment)
  MATCH (message)-[:REPLY_OF_COMMENT*0..]->(root:Comment)-[:REPLY_OF_POST]->(post:Post)
  MATCH (post)-[:HAS_CREATOR]->(originalPoster:Person)
  RETURN message, post, originalPoster
}
RETURN "IS2" AS query,
       message.id,
       coalesce(message.content, message.imageFile) AS content,
       message.creationDate,
       post.id,
       originalPoster.id,
       originalPoster.firstName,
       originalPoster.lastName
ORDER BY message.creationDate DESC, message.id DESC
LIMIT 10;

// IS3
MATCH (p:Person {id: $personId})-[k:KNOWS]-(friend:Person)
RETURN "IS3" AS query,
       friend.id,
       friend.firstName,
       friend.lastName,
       k.creationDate
ORDER BY k.creationDate DESC, friend.id ASC;

// IS4
CALL {
  MATCH (m:Post {id: $messageId})
  RETURN m
  UNION ALL
  MATCH (m:Comment {id: $messageId})
  RETURN m
}
RETURN "IS4" AS query,
       m.creationDate,
       coalesce(m.content, m.imageFile) AS content;

// IS5
CALL {
  MATCH (m:Post {id: $messageId})-[:HAS_CREATOR]->(creator:Person)
  RETURN creator
  UNION ALL
  MATCH (m:Comment {id: $messageId})-[:HAS_CREATOR]->(creator:Person)
  RETURN creator
}
RETURN "IS5" AS query, creator.id, creator.firstName, creator.lastName;

// IS6
CALL {
  MATCH (message:Post {id: $messageId})
  RETURN message
  UNION ALL
  MATCH (message:Comment {id: $messageId})
  RETURN message
}
CALL {
  WITH message
  MATCH (message:Post)-[:IS_CONTAINED_IN]->(forum:Forum)
  RETURN forum
  UNION ALL
  WITH message
  MATCH (message:Comment)-[:REPLY_OF_COMMENT*0..]->(root:Comment)-[:REPLY_OF_POST]->(post:Post)-[:IS_CONTAINED_IN]->(forum:Forum)
  RETURN forum
}
MATCH (forum)-[:HAS_MODERATOR]->(moderator:Person)
RETURN "IS6" AS query,
       forum.id,
       forum.title,
       moderator.id,
       moderator.firstName,
       moderator.lastName;

// IS7
CALL {
  MATCH (message:Post {id: $messageId})
  RETURN message
  UNION ALL
  MATCH (message:Comment {id: $messageId})
  RETURN message
}
MATCH (message)-[:HAS_CREATOR]->(messageAuthor:Person)
MATCH (comment:Comment)
WHERE (comment)-[:REPLY_OF_POST]->(message)
   OR (comment)-[:REPLY_OF_COMMENT]->(message)
MATCH (comment)-[:HAS_CREATOR]->(replyAuthor:Person)
RETURN "IS7" AS query,
       comment.id,
       comment.content,
       comment.creationDate,
       replyAuthor.id,
       replyAuthor.firstName,
       replyAuthor.lastName,
       CASE
         WHEN replyAuthor = messageAuthor THEN false
         WHEN (replyAuthor)-[:KNOWS]-(messageAuthor) THEN true
         ELSE false
       END AS knows
ORDER BY comment.creationDate DESC, replyAuthor.id ASC;
