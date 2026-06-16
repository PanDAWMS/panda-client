import dash_core_components as dcc
import dash_html_components as html
import dash_table
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash.dependencies import Input, Output
from jupyter_dash import JupyterDash

from . import Client


def show_task(jeditaskid, verbose=False, mode="inline"):
    # get task
    task = Client.get_tasks_detailed_info_since(filters={"jediTaskID": jeditaskid}, verbose=verbose)[1][0]
    # get tasks of the user
    tasks = Client.get_tasks_detailed_info_since(filters={"userName": task["userName"]}, verbose=verbose)[1]
    tids = {x["jediTaskID"] for x in tasks}
    tids.add(jeditaskid)

    # Build App
    app = JupyterDash(__name__)
    app.layout = html.Div(
        [
            html.Div(
                [
                    html.H2("TaskID: "),
                    dcc.Dropdown(id="dropdown_taskid", options=[{"label": i, "value": i} for i in tids], value=jeditaskid),
                ],
                style={"display": "inline-block", "width": "20%"},
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.H2("Task Attributes"),
                            dash_table.DataTable(
                                id="00_table",
                                columns=[{"id": "attribute", "name": "attribute"}, {"id": "value", "name": "value"}],
                                page_action="none",
                                style_table={"height": "330px", "overflowY": "auto"},
                                style_cell_conditional=[
                                    {"if": {"column_id": "value"}, "textAlign": "left"},
                                ],
                            ),
                        ],
                        style={"display": "inline-block", "width": "49%", "float": "left", "padding-top": "30px"},
                    ),
                    html.Div(
                        [
                            dcc.Graph(id="01_graph"),
                        ],
                        style={"display": "inline-block", "width": "49%"},
                    ),
                ],
            ),
            html.Div(
                [
                    html.Div([dcc.Graph(id="10_graph")], style={"display": "inline-block", "width": "49%"}),
                    html.Div([dcc.Graph(id="11_graph")], style={"display": "inline-block", "width": "49%"}),
                ],
            ),
        ]
    )

    # Run app and display result inline in the notebook
    app.run_server(mode=mode)

    @app.callback(
        Output("00_table", "data"), Output("01_graph", "figure"), Output("10_graph", "figure"), Output("11_graph", "figure"), Input("dropdown_taskid", "value")
    )
    def make_elements(jeditaskid):
        verbose = False
        task = Client.get_tasks_detailed_info_since(filters={"jediTaskID": jeditaskid}, verbose=verbose)[1][0]
        jobs = Client.get_job_descriptions(jeditaskid, verbose=verbose)[1]
        jobs = pd.DataFrame(jobs)

        # task data
        task_data = [{"attribute": k, "value": task[k]} for k in task if isinstance(task[k], (str, type(None)))]

        # figures
        site_fig = px.histogram(jobs, x="computingSite", color="jobStatus")
        ram_fig = px.histogram(jobs, x="maxRSS")

        exectime_fig = go.Figure()
        legend_set = set()
        for d in jobs.itertuples(index=False):
            if d.jobStatus == "finished":
                t_color = "green"
            elif d.jobStatus == "failed":
                t_color = "red"
            else:
                t_color = "orange"
            if d.jobStatus not in legend_set:
                show_legend = True
                legend_set.add(d.jobStatus)
                exectime_fig.add_trace(
                    go.Scatter(
                        x=[d.creationTime, d.creationTime],
                        y=[d.PandaID, d.PandaID],
                        mode="lines",
                        line=go.scatter.Line(color=t_color),
                        showlegend=True,
                        legendgroup=d.jobStatus,
                        name=d.jobStatus,
                        hoverinfo="skip",
                    )
                )
            exectime_fig.add_trace(
                go.Scatter(
                    x=[d.creationTime, d.endTime],
                    y=[d.PandaID, d.PandaID],
                    mode="lines",
                    line=go.scatter.Line(color=t_color),
                    showlegend=False,
                    legendgroup=d.jobStatus,
                    name="",
                    hovertemplate="PandaID: %{y:d}",
                )
            )
        exectime_fig.update_xaxes(range=[jobs["creationTime"].min(), jobs["endTime"].max()], title_text="Job Lifetime")
        exectime_fig.update_yaxes(range=[jobs["PandaID"].min() * 0.999, jobs["PandaID"].max() * 1.001], title_text="PandaID")

        return task_data, site_fig, ram_fig, exectime_fig
