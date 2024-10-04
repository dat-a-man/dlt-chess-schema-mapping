import dlt
from chess import source



def load_players_games_example(start_month: str, end_month: str) -> None:
    """Constructs a pipeline that will load chess games of specific players for a range of months."""

    # configure the pipeline: provide the destination and dataset name to which the data should go
    pipeline = dlt.pipeline(
        pipeline_name="chess_pipeline1",
        destination='bigquery',
        dataset_name="chess_data_tst",
    )
    # create the data source by providing a list of players and start/end month in YYYY/MM format
    data = source(
        [
    "JaloliddinIlkhomi",
    "Blitz-Warrior1993",
    "Sanan_Sjugirov",
    "Buffy7",
    "Sakanelli",
    "grzechu96",
    "FL0RIAN0P0LIS",
    "mbojan",
    "stollenmonster",
    "Topspin111",
    "Mykola-Bortnyk",
    "ArkadiyKhromaev777",
    "luffygear7",
    "Azeryahu",
    "mac0504",
    "IGROK1111",
    "Aqueen055",
    "tom740",
    "oknvs",
    "vi_pranav",
    "Salem-AR",
    "Bigfish1995",
    "SaqoChess_Coach",
    "Arystanner",
    "shimastream",
    "Oleksandr_Bortnyk",
    "BogdanDeac",
    "joppie2",
    "NikoTheodorou",
    "wonderfultime",
    "ChessBrah",
    "DenLaz",
    "howitzer14",
    "RadoslawPsyk",
    "ChessWeebs",
    "Blitzstream",
    "alexrustemov",
    "GHANDEEVAM2003",
    "seramoral",
    "Grandmaster2B",
    "jlucasvf",
    "francyIM",
    "BigAl",
    "Legendinunknown",
    "severomorskij",
    "daika91",
    "ChessQueen",
    "Petrovic_Aleksa",
    "c63_amg",
    "ChessShop",
    "Orange_Ghost",
    "GMKrikor",
    "HSH1987",
    "larsmandreassen",
    "TheRealGmochey",
    "MokshAD1603",
    "eljanov",
    "Vladimir_Zakhartsov",
    "Vaibhavraut99",
    "AChucky",
    "1tsNowOrNever",
    "inneremptiness",
    "theexpertpatzer",
    "Kempsy",
    "TrahtarBelarus",
    "Twitch_ElhamBlitz05",
    "thatsallshewrote",
    "CMmauricio",
    "VitorBronson",
    "Aradhya2000",
    "Cayse",
    "Grandelicious",
    "TigrVShlyape",
    "biostatistician",
    "fm9714",
    "AlexPapasimakopoulos",
    "Arek_pacitan",
    "Vahe_Danielyan",
    "Msb2",
    "GMWSO",
    "daro94",
    "Hikaru",
    "Jospem",
    "Njal28",
    "Duhless",
    "Byniolus",
    "Konavets",
    "tptagain",
    "DetskiSad",
    "LyonBeast",
    "mishanick",
    "SantoBlue",
    "nihalsarin",
    "penguingm1",
    "rpragchess",
    "dropstoneDP",
    "AnthonyWirig",
    "TrimitziosP7",
    "vaishali2001",
    "WiniVidiVici",
    "MagnusCarlsen",
    "ManuDavid2910",
    "Semyon_Khanin",
    "VincentKeymer",
    "exoticprincess",
    "FabianoCaruana",
    "LacusSomniorum",
    "BrandonJacobson",
    "GukeshDommaraju",
    "DanielNaroditsky",
    "kirillshevchenko",
    "spicycaterpillar",
    "RaunakSadhwani2005",
        ],
        start_month=start_month,
        end_month=end_month,
    )
    # load the "players_games" and "players_profiles" out of all the possible resources
    info = pipeline.run(data.with_resources("players_games", "players_profiles"))
    print(info)


def load_players_online_status() -> None:
    """Constructs a pipeline that will append online status of selected players"""

    pipeline = dlt.pipeline(
        pipeline_name="chess_pipeline1",
        destination='bigquery',
        dataset_name="chess_data_tst",
    )
    data = source(["magnuscarlsen", "vincentkeymer", "dommarajugukesh", "rpragchess"])
    info = pipeline.run(data.with_resources("players_online_status"))
    print(info)


def load_players_games_incrementally() -> None:
    """Pipeline will not load the same game archive twice"""
    # loads games for 11.2022
    load_players_games_example("2022/11", "2022/11")
    # second load skips games for 11.2022 but will load for 12.2022
    load_players_games_example("2022/11", "2022/12")


if __name__ == "__main__":
    # run our main example
    load_players_games_example("2022/11", "2022/12")
    load_players_online_status()
