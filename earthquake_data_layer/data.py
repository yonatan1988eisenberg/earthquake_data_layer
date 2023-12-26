import json
from earthquake_data_layer import definitions


class Data:

    @classmethod
    def get_latest_update(cls) -> tuple[str, int]:
        """
        loads the latest_update date and offset from the metadata file
        """
        metadata = cls.get_metadate()
        latest_update = metadata.get("latest_update", {})
        date = latest_update.get("date")
        offset = latest_update.get("offset")

        if not date:
            date = "1638-01-01"

        if not offset:
            offset = 1

        return date, offset

    @classmethod
    def update_latest_update(cls, date: str, offset: int) -> bool:
        """
        updates the latest_update in the metadate file
        """
        metadata = cls.get_metadate()
        metadata.update({"latest_update": {"date": date, "offset": offset}})
        with open(definitions.METADATA_LOCATION, "w") as file:
            json.dump(metadata, file)

        return True

    @staticmethod
    def get_metadate() -> dict:
        with open(definitions.METADATA_LOCATION, "r") as file:
            metadata = json.load(file)
        return metadata
