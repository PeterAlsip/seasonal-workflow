import sys, yaml
name = yaml.load(open("/home/Peter.Alsip/git/seasonal-workflow/config_greatlakes_physics.yaml"),Loader=yaml.BaseLoader)["name"]
lake = yaml.load(open("/home/Peter.Alsip/git/seasonal-workflow/workflows/retrospective_workflow.yaml"),Loader=yaml.BaseLoader)["workflow"]["entities"]["LAKE"]

if name != lake:
    sys.exit(f"Error: entity LAKE ({lake}) in retrospective_workflow.yaml must match name ({name}) in config.yaml")

