"""TEEHR evaluation in the shipped registry (hydrowf-core docs/design/teehr-evaluation.md): ngiab-to-cng 0.4.0
writes teehr-timeseries, teehr-evaluate reads it in place, from the step before it (ngiab-evaluation) or from
a caller's earlier job (teehr-evaluation)."""
import json
from pathlib import Path

import pytest

from hydrowf_core.adapters.argo import BadInputs, Reader, render
from hydrowf_core.adapters.ogc import process_description
from hydrowf_ogc.registry import find_pipeline, load_legos

SHIPPED = Path(__file__).parents[1] / "registry" / "legos.yaml"
READER = Reader(image="curlimages/curl@sha256:" + "a" * 64, vendor_url="http://hydrowf-ogc.hydrowf.svc:8001/step-keys",
                bucket="my-bucket", s3_endpoint="http://minio.argo.svc:9000")
EVALUATION = {"feature": "gage-01031510", "start_date": "2020-06-01", "end_date": "2020-06-30",
              "hydrofabric": "s3://my-bucket/hydrofabric/v2.2/conus_nextgen.gpkg"}
EARLIER = "s3://my-bucket/job-0/to-teehr/cng/"              # where an ngiab-evaluation job writes it (core D.7)


def rendered(pipeline_id, inputs, version="0.1.0", **kw):
    legos = load_legos(SHIPPED)                  # runs hydrowf_core.registry.check on the whole file
    return render(find_pipeline(legos, pipeline_id, version), legos, inputs, lambda image: None,
                  **{"name": "job-1", "reader": READER, **kw})


def template(workflow, step):
    return next(t for t in workflow["spec"]["templates"] if t["name"] == f"step-{step}")


def reads(workflow):
    return json.loads(workflow["metadata"]["annotations"]["hydrowf/reads"])


def test_the_shipped_registry_has_both_evaluation_pipelines():
    legos = load_legos(SHIPPED)
    pipelines = {lego.id for lego in legos if lego.kind == "pipeline"}
    assert {"ngiab-evaluation", "teehr-evaluation"} <= pipelines


def test_ngiab_evaluation_converts_to_teehr_timeseries_and_evaluates_it_in_place():
    workflow = rendered("ngiab-evaluation", EVALUATION)
    to_teehr, evaluate = template(workflow, "to-teehr"), template(workflow, "evaluate")
    args = to_teehr["container"]["args"]
    assert args[args.index("--format") + 1] == "teehr-timeseries"
    [cng] = to_teehr["outputs"]["artifacts"]
    assert cng["s3"]["key"] == "job-1/to-teehr/cng/"                        # a key render knows (core D.7)
    assert evaluate["container"]["args"][0] == "s3://my-bucket/job-1/to-teehr/cng/"
    assert reads(workflow) == {"evaluate": ["job-1/to-teehr/cng/"]}
    assert json.loads(workflow["metadata"]["annotations"]["hydrowf/formats"])["timeseries"].startswith(
        "teehr-timeseries@")


def test_teehr_evaluate_runs_through_its_entrypoint_with_its_defaults():
    evaluate = template(rendered("ngiab-evaluation", EVALUATION), "evaluate")
    container = evaluate["container"]
    assert container["image"].startswith("ghcr.io/aquaveo/teehr-evaluate@sha256:")
    # the whole ENTRYPOINT: entrypoint.sh names a uid the image doesn't (Hadoop needs a user name)
    assert container["command"] == ["/app/entrypoint.sh", "python", "/app/teehr_evaluate.py"]
    args = container["args"]
    assert args[args.index("--metrics") + 1] == "kge,nse,relative_bias,rmsdr"
    assert args[args.index("--baseline") + 1] == "none"
    assert container["resources"]["limits"] == {"memory": "6144Mi"}       # Spark: 3.6 GiB measured at peak


def test_the_baseline_and_metrics_are_the_callers_choice():
    workflow = rendered("ngiab-evaluation", {**EVALUATION, "metrics": "kge,pearson",
                                             "baseline": "nwm30_retrospective"})
    args = template(workflow, "evaluate")["container"]["args"]
    assert args[args.index("--metrics") + 1] == "kge,pearson"
    assert args[args.index("--baseline") + 1] == "nwm30_retrospective"


@pytest.mark.parametrize("metrics", ["knee", "kge,", "kge nse", "-kge", "kge;rm -rf /", ""])
def test_metrics_outside_the_tools_list_are_refused(metrics):
    with pytest.raises(BadInputs):
        rendered("ngiab-evaluation", {**EVALUATION, "metrics": metrics})


def test_a_baseline_the_tool_doesnt_have_is_refused():
    with pytest.raises(BadInputs):
        rendered("ngiab-evaluation", {**EVALUATION, "baseline": "nwm21_retrospective"})


def test_teehr_evaluation_reads_a_callers_earlier_output_in_place():
    workflow = rendered("teehr-evaluation", {"timeseries": EARLIER})
    assert template(workflow, "evaluate")["container"]["args"][0] == EARLIER
    assert reads(workflow) == {"evaluate": ["job-0/to-teehr/cng/"]}


@pytest.mark.parametrize("metrics", ["knee", "kge;rm -rf /"])
def test_teehr_evaluation_refuses_metrics_outside_the_tools_list(metrics):
    with pytest.raises(BadInputs):
        rendered("teehr-evaluation", {"timeseries": EARLIER, "metrics": metrics})


def test_teehr_evaluate_works_on_its_own_workdir():
    evaluate = template(rendered("teehr-evaluation", {"timeseries": EARLIER}), "evaluate")
    env = {e["name"]: e["value"] for e in evaluate["container"]["env"]}
    assert env["HOME"] == env["TMPDIR"] == "/work"                    # render's emptyDir, not /tmp in the image


@pytest.mark.parametrize("version", ["0.1.0", "0.2.0"])
@pytest.mark.parametrize("pipeline_id", ["ngiab-evaluation", "teehr-evaluation"])
def test_the_process_description_shows_the_metrics_pattern_and_the_baselines(pipeline_id, version):
    legos = load_legos(SHIPPED)
    inputs = process_description(find_pipeline(legos, pipeline_id, version), legos)["inputs"]
    assert inputs["metrics"]["schema"]["pattern"].startswith("^(?:(kge|kge_mod1|")
    assert inputs["baseline"]["schema"]["enum"] == ["none", "nwm30_retrospective"]


# ---- 0.2.0: ngiab-to-cng 0.4.1 (refuses TEEHR's own configuration names) and teehr-evaluate 0.1.1 (drops a
# repeated metric); the 0.1.0 pipelines above stay as published

def image(workflow, step):
    return template(workflow, step)["container"]["image"]


def test_the_0_2_0_pipelines_run_the_follow_up_tools():
    ngiab = rendered("ngiab-evaluation", EVALUATION, version="0.2.0")
    assert image(ngiab, "to-teehr").endswith("@sha256:71fb90d98d8b86181804288701396afee6726b6241ff637559e662d8d5c3d4ae")
    teehr = rendered("teehr-evaluation", {"timeseries": EARLIER}, version="0.2.0")
    assert image(ngiab, "evaluate") == image(teehr, "evaluate")
    assert image(teehr, "evaluate") != image(rendered("teehr-evaluation", {"timeseries": EARLIER}), "evaluate")


@pytest.mark.parametrize("name", ["usgs_observations", "nwm30_retrospective", "cfe", "Ngiab", "ngiab cfe"])
def test_ngiab_evaluation_0_2_0_refuses_a_name_not_starting_with_ngiab_at_submission(name):
    with pytest.raises(BadInputs):                     # render refuses before any step exists: NextGen never runs
        rendered("ngiab-evaluation", {**EVALUATION, "configuration_name": name}, version="0.2.0")


@pytest.mark.parametrize("name", ["ngiab", "ngiab_cfe_default", "ngiab-cfe-v2"])
def test_ngiab_evaluation_0_2_0_passes_an_ngiab_name_to_the_tool(name):
    args = template(rendered("ngiab-evaluation", {**EVALUATION, "configuration_name": name}, version="0.2.0"),
                    "to-teehr")["container"]["args"]
    assert args[args.index("--configuration-name") + 1] == name


def test_a_repeated_metric_is_accepted_and_left_to_the_tool_to_drop():
    # the contract teehr-evaluate 0.1.1 relies on: the registry doesn't refuse a repeat; the tool drops it
    args = template(rendered("teehr-evaluation", {"timeseries": EARLIER, "metrics": "kge,nse,kge"}, version="0.2.0"),
                    "evaluate")["container"]["args"]
    assert args[args.index("--metrics") + 1] == "kge,nse,kge"


def test_the_process_description_shows_both_configuration_name_patterns():
    legos = load_legos(SHIPPED)
    schema = process_description(find_pipeline(legos, "ngiab-evaluation", "0.2.0"), legos)["inputs"][
        "configuration_name"]["schema"]
    assert schema["allOf"] == [{"pattern": "^(?:ngiab[A-Za-z0-9_-]*)$"}, {"pattern": "^(?:[A-Za-z0-9][A-Za-z0-9_-]*)$"}]


def same_but_version(lego):
    """A tool lego without what a release changes: its version, its image and its ports' descriptions."""
    dumped = lego.model_dump(exclude={"version", "image"})
    for port in dumped["inputs"] + dumped["outputs"]:
        port.pop("description", None)
    return dumped


@pytest.mark.parametrize("tool_id, published, follow_up", [("ngiab-to-cng", "0.4.0", "0.4.1"),
                                                           ("teehr-evaluate", "0.1.0", "0.1.1")])
def test_a_follow_up_tool_changes_only_its_image(tool_id, published, follow_up):
    tools = {(x.id, x.version): x for x in load_legos(SHIPPED) if x.kind == "tool"}
    assert same_but_version(tools[tool_id, published]) == same_but_version(tools[tool_id, follow_up])
    assert tools[tool_id, published].image != tools[tool_id, follow_up].image
