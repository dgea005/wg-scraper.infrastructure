DROP TABLE IF EXISTS index_stg;
CREATE TABLE index_stg 
(
    id INTEGER,
    listing_id INTEGER DEFAULT NULL,
    link TEXT DEFAULT NULL,
    cost SMALLINT DEFAULT NULL,
    size SMALLINT DEFAULT NULL
);


-- should create index here

--
-- Function and trigger to amend summarized column(s) on UPDATE, INSERT, DELETE.
--
CREATE OR REPLACE FUNCTION index_raw_to_stg() RETURNS TRIGGER
AS $index_raw_to_stg$
    DECLARE
        delta_id                      integer;
        delta_listing_id              integer;
        delta_link                    text;
        delta_cost                    smallint;
        delta_size                    smallint;
    BEGIN

        delta_id = NEW.id;
        delta_listing_id = NEW.listing_id;
        delta_link = NEW.link;
        delta_cost = NEW.cost;
        delta_size = NEW.size;

        -- Insert or update the summary row with the new values.
        <<insert_update>>
        LOOP
            
            BEGIN
                INSERT INTO index_stg (
                            id,
                            listing_id,
                            link,
                            cost,
                            size)
                    VALUES (
                            delta_id,
                            delta_listing_id,
                            delta_link,
                            delta_cost,
                            delta_size
                           );

                EXIT insert_update;

            END;
        END LOOP insert_update;

        RETURN NULL;

    END;
$index_raw_to_stg$ LANGUAGE plpgsql;

CREATE TRIGGER index_raw_to_stg
AFTER INSERT ON index_raw
    FOR EACH ROW EXECUTE PROCEDURE index_raw_to_stg();
