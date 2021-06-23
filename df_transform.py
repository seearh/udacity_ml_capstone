import pandas as pd



def get_offer_periods(df, portfolio):
    """
    Takes in transaction data for one person and offer data
    Return a DataFrame of offer periods with start, end, view & complete time
    """
    # Split DataFrame by event
    df_received = df[df["event"] == "offer received"][["time", "offer_id"]].rename(columns={"time": "start_time"})
    df_viewed = df[df["event"] == "offer viewed"][["offer_id", "time"]].rename(columns={"time": "view_time"})
    df_completed = df[df["event"] == "offer completed"][["offer_id", "time"]].rename(columns={"time": "complete_time"})
    
    # Calculate offer end time by adding offer duration (in days)
    df = df_received.merge(portfolio[["offer_id", "duration"]], on="offer_id", how="left")
    df["end_time"] = df["start_time"] + df["duration"] * 24
    df.drop(columns="duration", inplace=True)

    # Find unique offer view time (if any) corresponding to each valid offer period
    df = df.merge(df_viewed, on="offer_id", how="left")
    df = df[
        df["view_time"].isna() |
        (
            (df["start_time"] <= df["view_time"]) &
            (df["end_time"] >= df["view_time"])
        )
    ]
    df = df.groupby(by=["start_time", "offer_id", "end_time"]).first().reset_index()

    # Find unique offer complete time (if any) corresponding to each valid offer period
    df = df.merge(df_completed, on="offer_id", how="left")
    df = df[
        df["view_time"].isna() |
        df["complete_time"].isna() |
        (
            (df["view_time"] <= df["complete_time"]) &
            (df["end_time"] >= df["complete_time"])
        )
    ]
    df = df.groupby(by=["start_time", "offer_id", "end_time"]).first().reset_index()
    
    return df


def get_influence_periods(grouped, portfolio, start_time, end_time):
    """
    grouped (tuple): ID of person and transcript DataFrame associated with him
    start_time (int): Experiment start time
    end_time (int): Experiment start time
    Returns (pandas.DataFrame): Offer influence periods and uninfluenced periods of the person, 
                                from experiment start till end time
    """
    group, df = grouped
    offer_periods = get_offer_periods(df, portfolio)
    # Offer influence period is from view_time till complete_time (completed) or end_time (not completed)
    offer_periods['start_time'] = offer_periods['view_time']
    offer_periods['end_time'] = offer_periods['end_time'].where(
        offer_periods['complete_time'].isna(), 
        offer_periods['complete_time']
    )
    offer_periods = offer_periods[['start_time', 'end_time', 'offer_id']].dropna()
    # Cuts off at overall end_time
    offer_periods['end_time'] = offer_periods['end_time'].where(offer_periods['end_time'] <= end_time, end_time)
    # Filter out offers viewed after completion 
    offer_periods = offer_periods[offer_periods['start_time'] <= offer_periods['end_time']]
    
    # Generate non-influence periods by filling in the gaps in offer influence periods
    no_offer_periods = pd.DataFrame(
        fill_timeline_gaps(offer_periods, start_time, end_time),
        columns=['start_time', 'end_time']
    )
    no_offer_periods['offer_id'] = "no_offer"
    
    periods = pd.concat([offer_periods, no_offer_periods], axis=0).sort_values(by=['start_time', 'end_time'])
    periods['person'] = group

    return periods


def fill_timeline_gaps(df, start_time, end_time):
    """
    df (pandas.DataFrame): Influence periods with start_time, end_time
    start_time (int): Experiment start time
    end_time (int): Experiment start time
    Yields (tuple): Time periods where user is under no offer influence
    """
    time = start_time
    while time < end_time:
        try:
            # Find next offer period start time
            row = df.loc[df["start_time"] >= time].iloc[0]
        except IndexError:
            # No more offer periods till end time
            yield (time, end_time)
            time = end_time
        else:
            # Keep looking at next offer period until view time != complete time
            ind = 1
            # Edge case: view time = complete time
            while row['start_time'] == row['end_time']:
                # Yield uninfluenced period up till this view time, and refresh uninfluenced period
                yield (time, row['start_time'])
                time = row['end_time']
                try:
                    row = df.loc[df["start_time"] >= time].iloc[ind]
                # No more offer periods till end time
                except IndexError:
                    yield (time, end_time)
                    time = end_time
                    break
                else:
                    ind += 1
            else:
                # Edge case: view time = experiment start time
                # First period is not uninfluenced, do not yield
                if row['start_time'] == start_time:
                    time = row['end_time']
                else:
                    #import pdb; pdb.set_trace()
                    yield (time, row['start_time'])
                    time = row['end_time']