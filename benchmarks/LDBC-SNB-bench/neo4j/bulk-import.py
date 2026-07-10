#!/usr/bin/env python3
import csv
import os
from pathlib import Path

CSV_ROOT = Path(os.environ["CSV_ROOT"])
OUT_DIR = Path(os.environ["IMPORT_DIR"])

OUT_DIR.mkdir(parents=True, exist_ok=True)


def part_files(subdir):
    files = sorted((CSV_ROOT / subdir).glob("part-*.csv"))
    if not files:
        raise SystemExit(f"No CSV files found under {CSV_ROOT / subdir}")
    return files


def get_val(row, idx, key):
    pos = idx.get(key)
    if pos is None or pos >= len(row):
        return ""
    return row[pos]


def is_deleted(row, idx):
    val = get_val(row, idx, "explicitlyDeleted").strip().lower()
    return val == "true"


def process_files(subdir, handler):
    for file in part_files(subdir):
        with file.open(newline="") as f:
            reader = csv.reader(f, delimiter="|")
            header = next(reader)
            idx = {name: i for i, name in enumerate(header)}
            for row in reader:
                handler(row, idx)


def write_nodes(label, subdir, header, cols, explicit=False, deleted_ids=None):
    out_path = OUT_DIR / f"{label}.nodes.csv"
    with out_path.open("w", newline="") as f:
        writer = csv.writer(f, delimiter="|", lineterminator="\n")
        writer.writerow(header)

        def handle(row, idx):
            row_id = get_val(row, idx, cols[0]).strip()
            if explicit and is_deleted(row, idx):
                if deleted_ids is not None and row_id:
                    deleted_ids.add(row_id)
                return
            if not row_id:
                return
            writer.writerow([get_val(row, idx, c) for c in cols])

        process_files(subdir, handle)
    print(f"Wrote {out_path}")


def write_rels(name, subdir, header, row_builder, explicit=False):
    out_path = OUT_DIR / f"{name}.rel.csv"
    with out_path.open("w", newline="") as f:
        writer = csv.writer(f, delimiter="|", lineterminator="\n")
        writer.writerow(header)

        def handle(row, idx):
            if explicit and is_deleted(row, idx):
                return
            out_row = row_builder(row, idx)
            if out_row is None:
                return
            writer.writerow(out_row)

        process_files(subdir, handle)
    print(f"Wrote {out_path}")


def non_blank(val):
    return val is not None and val.strip() != ""


def id_in(deleted_ids, value):
    return value.strip() in deleted_ids


def main():
    deleted_person_ids = set()
    deleted_forum_ids = set()
    deleted_post_ids = set()
    deleted_comment_ids = set()
    # Nodes
    write_nodes(
        "Place",
        "static/Place",
        ["placeId:ID(Place)", "id:long", "name", "url", "type"],
        ["id", "id", "name", "url", "type"],
    )
    write_nodes(
        "Organisation",
        "static/Organisation",
        ["orgId:ID(Organisation)", "id:long", "type", "name", "url"],
        ["id", "id", "type", "name", "url"],
    )
    write_nodes(
        "TagClass",
        "static/TagClass",
        ["tagClassId:ID(TagClass)", "id:long", "name", "url"],
        ["id", "id", "name", "url"],
    )
    write_nodes(
        "Tag",
        "static/Tag",
        ["tagId:ID(Tag)", "id:long", "name", "url"],
        ["id", "id", "name", "url"],
    )
    write_nodes(
        "Person",
        "dynamic/Person",
        [
            "personId:ID(Person)",
            "id:long",
            "firstName",
            "lastName",
            "gender",
            "birthday:date",
            "creationDate:datetime",
            "locationIP",
            "browserUsed",
            "speaks:string[]",
            "email:string[]",
        ],
        [
            "id",
            "id",
            "firstName",
            "lastName",
            "gender",
            "birthday",
            "creationDate",
            "locationIP",
            "browserUsed",
            "language",
            "email",
        ],
        explicit=True,
        deleted_ids=deleted_person_ids,
    )
    write_nodes(
        "Forum",
        "dynamic/Forum",
        ["forumId:ID(Forum)", "id:long", "title", "creationDate:datetime"],
        ["id", "id", "title", "creationDate"],
        explicit=True,
        deleted_ids=deleted_forum_ids,
    )
    write_nodes(
        "Post",
        "dynamic/Post",
        [
            "postId:ID(Post)",
            "id:long",
            "imageFile",
            "locationIP",
            "browserUsed",
            "language",
            "content",
            "length:int",
            "creationDate:datetime",
        ],
        ["id", "id", "imageFile", "locationIP", "browserUsed", "language", "content", "length", "creationDate"],
        explicit=True,
        deleted_ids=deleted_post_ids,
    )
    write_nodes(
        "Comment",
        "dynamic/Comment",
        [
            "commentId:ID(Comment)",
            "id:long",
            "locationIP",
            "browserUsed",
            "content",
            "length:int",
            "creationDate:datetime",
        ],
        ["id", "id", "locationIP", "browserUsed", "content", "length", "creationDate"],
        explicit=True,
        deleted_ids=deleted_comment_ids,
    )

    # Relationships from static node files
    write_rels(
        "IS_PART_OF",
        "static/Place",
        [":START_ID(Place)", ":END_ID(Place)"],
        lambda row, idx: (
            [get_val(row, idx, "id"), get_val(row, idx, "PartOfPlaceId")]
            if non_blank(get_val(row, idx, "PartOfPlaceId"))
            else None
        ),
    )
    write_rels(
        "IS_LOCATED_IN",
        "static/Organisation",
        [":START_ID(Organisation)", ":END_ID(Place)"],
        lambda row, idx: (
            [get_val(row, idx, "id"), get_val(row, idx, "LocationPlaceId")]
            if non_blank(get_val(row, idx, "LocationPlaceId"))
            else None
        ),
    )
    write_rels(
        "HAS_TYPE",
        "static/Tag",
        [":START_ID(Tag)", ":END_ID(TagClass)"],
        lambda row, idx: (
            [get_val(row, idx, "id"), get_val(row, idx, "TypeTagClassId")]
            if non_blank(get_val(row, idx, "TypeTagClassId"))
            else None
        ),
    )
    write_rels(
        "IS_SUBCLASS_OF",
        "static/TagClass",
        [":START_ID(TagClass)", ":END_ID(TagClass)"],
        lambda row, idx: (
            [get_val(row, idx, "id"), get_val(row, idx, "SubclassOfTagClassId")]
            if non_blank(get_val(row, idx, "SubclassOfTagClassId"))
            else None
        ),
    )

    # Relationships from dynamic node files
    write_rels(
        "PERSON_IS_LOCATED_IN",
        "dynamic/Person",
        [":START_ID(Person)", ":END_ID(Place)"],
        lambda row, idx: (
            [get_val(row, idx, "id"), get_val(row, idx, "LocationCityId")]
            if non_blank(get_val(row, idx, "LocationCityId"))
            else None
        ),
        explicit=True,
    )
    write_rels(
        "HAS_MODERATOR",
        "dynamic/Forum",
        [":START_ID(Forum)", ":END_ID(Person)"],
        lambda row, idx: (
            None
            if id_in(deleted_person_ids, get_val(row, idx, "ModeratorPersonId"))
            else (
                [get_val(row, idx, "id"), get_val(row, idx, "ModeratorPersonId")]
                if non_blank(get_val(row, idx, "ModeratorPersonId"))
                else None
            )
        ),
        explicit=True,
    )
    write_rels(
        "HAS_CREATOR_POST",
        "dynamic/Post",
        [":START_ID(Post)", ":END_ID(Person)"],
        lambda row, idx: (
            None
            if id_in(deleted_person_ids, get_val(row, idx, "CreatorPersonId"))
            else (
                [get_val(row, idx, "id"), get_val(row, idx, "CreatorPersonId")]
                if non_blank(get_val(row, idx, "CreatorPersonId"))
                else None
            )
        ),
        explicit=True,
    )
    write_rels(
        "CONTAINER_OF",
        "dynamic/Post",
        [":START_ID(Forum)", ":END_ID(Post)"],
        lambda row, idx: (
            None
            if id_in(deleted_forum_ids, get_val(row, idx, "ContainerForumId"))
            else (
                [get_val(row, idx, "ContainerForumId"), get_val(row, idx, "id")]
                if non_blank(get_val(row, idx, "ContainerForumId"))
                else None
            )
        ),
        explicit=True,
    )
    write_rels(
        "POST_IS_LOCATED_IN",
        "dynamic/Post",
        [":START_ID(Post)", ":END_ID(Place)"],
        lambda row, idx: (
            [get_val(row, idx, "id"), get_val(row, idx, "LocationCountryId")]
            if non_blank(get_val(row, idx, "LocationCountryId"))
            else None
        ),
        explicit=True,
    )
    write_rels(
        "HAS_CREATOR_COMMENT",
        "dynamic/Comment",
        [":START_ID(Comment)", ":END_ID(Person)"],
        lambda row, idx: (
            None
            if id_in(deleted_person_ids, get_val(row, idx, "CreatorPersonId"))
            else (
                [get_val(row, idx, "id"), get_val(row, idx, "CreatorPersonId")]
                if non_blank(get_val(row, idx, "CreatorPersonId"))
                else None
            )
        ),
        explicit=True,
    )
    write_rels(
        "COMMENT_IS_LOCATED_IN",
        "dynamic/Comment",
        [":START_ID(Comment)", ":END_ID(Place)"],
        lambda row, idx: (
            [get_val(row, idx, "id"), get_val(row, idx, "LocationCountryId")]
            if non_blank(get_val(row, idx, "LocationCountryId"))
            else None
        ),
        explicit=True,
    )
    write_rels(
        "REPLY_OF_POST",
        "dynamic/Comment",
        [":START_ID(Comment)", ":END_ID(Post)"],
        lambda row, idx: (
            None
            if id_in(deleted_post_ids, get_val(row, idx, "ParentPostId"))
            else (
                [get_val(row, idx, "id"), get_val(row, idx, "ParentPostId")]
                if non_blank(get_val(row, idx, "ParentPostId"))
                else None
            )
        ),
        explicit=True,
    )
    write_rels(
        "REPLY_OF_COMMENT",
        "dynamic/Comment",
        [":START_ID(Comment)", ":END_ID(Comment)"],
        lambda row, idx: (
            None
            if id_in(deleted_comment_ids, get_val(row, idx, "ParentCommentId"))
            else (
                [get_val(row, idx, "id"), get_val(row, idx, "ParentCommentId")]
                if non_blank(get_val(row, idx, "ParentCommentId"))
                else None
            )
        ),
        explicit=True,
    )

    # Relationships from edge files
    write_rels(
        "HAS_INTEREST",
        "dynamic/Person_hasInterest_Tag",
        [":START_ID(Person)", ":END_ID(Tag)"],
        lambda row, idx: (
            None
            if id_in(deleted_person_ids, get_val(row, idx, "PersonId"))
            else (
                [get_val(row, idx, "PersonId"), get_val(row, idx, "TagId")]
                if non_blank(get_val(row, idx, "PersonId")) and non_blank(get_val(row, idx, "TagId"))
                else None
            )
        ),
    )
    write_rels(
        "KNOWS",
        "dynamic/Person_knows_Person",
        [":START_ID(Person)", ":END_ID(Person)", "creationDate:datetime"],
        lambda row, idx: (
            None
            if id_in(deleted_person_ids, get_val(row, idx, "Person1Id"))
            or id_in(deleted_person_ids, get_val(row, idx, "Person2Id"))
            else (
                [get_val(row, idx, "Person1Id"), get_val(row, idx, "Person2Id"), get_val(row, idx, "creationDate")]
                if non_blank(get_val(row, idx, "Person1Id")) and non_blank(get_val(row, idx, "Person2Id"))
                else None
            )
        ),
        explicit=True,
    )
    write_rels(
        "STUDY_AT",
        "dynamic/Person_studyAt_University",
        [":START_ID(Person)", ":END_ID(Organisation)", "classYear:int"],
        lambda row, idx: (
            None
            if id_in(deleted_person_ids, get_val(row, idx, "PersonId"))
            else (
                [get_val(row, idx, "PersonId"), get_val(row, idx, "UniversityId"), get_val(row, idx, "classYear")]
                if non_blank(get_val(row, idx, "PersonId")) and non_blank(get_val(row, idx, "UniversityId"))
                else None
            )
        ),
    )
    write_rels(
        "WORK_AT",
        "dynamic/Person_workAt_Company",
        [":START_ID(Person)", ":END_ID(Organisation)", "workFrom:int"],
        lambda row, idx: (
            None
            if id_in(deleted_person_ids, get_val(row, idx, "PersonId"))
            else (
                [get_val(row, idx, "PersonId"), get_val(row, idx, "CompanyId"), get_val(row, idx, "workFrom")]
                if non_blank(get_val(row, idx, "PersonId")) and non_blank(get_val(row, idx, "CompanyId"))
                else None
            )
        ),
    )
    write_rels(
        "FORUM_HAS_TAG",
        "dynamic/Forum_hasTag_Tag",
        [":START_ID(Forum)", ":END_ID(Tag)"],
        lambda row, idx: (
            None
            if id_in(deleted_forum_ids, get_val(row, idx, "ForumId"))
            else (
                [get_val(row, idx, "ForumId"), get_val(row, idx, "TagId")]
                if non_blank(get_val(row, idx, "ForumId")) and non_blank(get_val(row, idx, "TagId"))
                else None
            )
        ),
    )
    write_rels(
        "HAS_MEMBER",
        "dynamic/Forum_hasMember_Person",
        [":START_ID(Forum)", ":END_ID(Person)", "joinDate:datetime"],
        lambda row, idx: (
            None
            if id_in(deleted_forum_ids, get_val(row, idx, "ForumId"))
            or id_in(deleted_person_ids, get_val(row, idx, "PersonId"))
            else (
                [get_val(row, idx, "ForumId"), get_val(row, idx, "PersonId"), get_val(row, idx, "creationDate")]
                if non_blank(get_val(row, idx, "ForumId")) and non_blank(get_val(row, idx, "PersonId"))
                else None
            )
        ),
        explicit=True,
    )
    write_rels(
        "POST_HAS_TAG",
        "dynamic/Post_hasTag_Tag",
        [":START_ID(Post)", ":END_ID(Tag)"],
        lambda row, idx: (
            None
            if id_in(deleted_post_ids, get_val(row, idx, "PostId"))
            else (
                [get_val(row, idx, "PostId"), get_val(row, idx, "TagId")]
                if non_blank(get_val(row, idx, "PostId")) and non_blank(get_val(row, idx, "TagId"))
                else None
            )
        ),
    )
    write_rels(
        "COMMENT_HAS_TAG",
        "dynamic/Comment_hasTag_Tag",
        [":START_ID(Comment)", ":END_ID(Tag)"],
        lambda row, idx: (
            None
            if id_in(deleted_comment_ids, get_val(row, idx, "CommentId"))
            else (
                [get_val(row, idx, "CommentId"), get_val(row, idx, "TagId")]
                if non_blank(get_val(row, idx, "CommentId")) and non_blank(get_val(row, idx, "TagId"))
                else None
            )
        ),
    )
    write_rels(
        "LIKES_POST",
        "dynamic/Person_likes_Post",
        [":START_ID(Person)", ":END_ID(Post)", "creationDate:datetime"],
        lambda row, idx: (
            None
            if id_in(deleted_person_ids, get_val(row, idx, "PersonId"))
            or id_in(deleted_post_ids, get_val(row, idx, "PostId"))
            else (
                [get_val(row, idx, "PersonId"), get_val(row, idx, "PostId"), get_val(row, idx, "creationDate")]
                if non_blank(get_val(row, idx, "PersonId")) and non_blank(get_val(row, idx, "PostId"))
                else None
            )
        ),
        explicit=True,
    )
    write_rels(
        "LIKES_COMMENT",
        "dynamic/Person_likes_Comment",
        [":START_ID(Person)", ":END_ID(Comment)", "creationDate:datetime"],
        lambda row, idx: (
            None
            if id_in(deleted_person_ids, get_val(row, idx, "PersonId"))
            or id_in(deleted_comment_ids, get_val(row, idx, "CommentId"))
            else (
                [get_val(row, idx, "PersonId"), get_val(row, idx, "CommentId"), get_val(row, idx, "creationDate")]
                if non_blank(get_val(row, idx, "PersonId")) and non_blank(get_val(row, idx, "CommentId"))
                else None
            )
        ),
        explicit=True,
    )


if __name__ == "__main__":
    main()
