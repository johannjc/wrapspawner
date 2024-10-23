# Copyright (c) Regents of the University of Minnesota
# Distributed under the terms of the Modified BSD License.

"""WrapSpawner classes

WrapSpawner provides a mechanism to wrap the interface of a Spawner such that
the Spawner class to use for single-user servers can be chosen dynamically.
The child Spawner is created and started using the same logic as in User.spawn(),
but not until start() or load_state() is called. Thus, subclasses may modify the
class or properties of the child Spawner at any earlier point (e.g. from
Authenticator pre_spawn hooks or options form processing).

Note that there is no straightforward way to simply save the child_class Type
value in the ORM, so a subclass that provides a way to change the child_class
must also arrange to reproduce that change when state is reloaded.

Provided as an initial application is ProfilesSpawner, which accepts a list
of provided Spawner configurations, and generates an options form from that list.
Using this mechanism, the administrator can provide users with a pre-approved
selection of Spawner configurations.
"""

import json
import re
import urllib.request

from tornado import concurrent

from jupyterhub.spawner import LocalProcessSpawner, Spawner
from traitlets import (
    Instance, Type, Tuple, List, Dict, Unicode, Any
)
from traitlets import directional_link, validate, TraitError

# Only needed for DockerProfilesSpawner
try:
    import docker
except ImportError:
    pass

# Utility to create dummy Futures to return values through yields
def _yield_val(x=None):
    f = concurrent.Future()
    f.set_result(x)
    return f

class WrapSpawner(Spawner):

    # Grab this from constructor args in case some Spawner ever wants it
    config = Any()

    child_class = Type(LocalProcessSpawner, Spawner,
        config=True,
        help="""The class to wrap for spawning single-user servers.
                Should be a subclass of Spawner.
                """
        )

    child_config = Dict(default_value={},
        config=True,
        help="Dictionary of config values to apply to wrapped spawner class."
        )

    child_state = Dict(default_value={})

    child_spawner = Instance(Spawner, allow_none=True)

    def construct_child(self):
        if self.child_spawner is None:
            self.child_spawner = self.child_class(
                user = self.user,
                db   = self.db,
                hub  = self.hub,
                authenticator = self.authenticator,
                oauth_client_id = self.oauth_client_id,
                cookie_options = self.cookie_options,
                orm_spawner = self.orm_spawner,
                server = self._server,
                config = self.config,
                **self.child_config
                )
            # initial state will always be wrong since it will see *our* state
            self.child_spawner.clear_state()
            if self.child_state:
                self.child_spawner.load_state(self.child_state)

            # link traits common between self and child
            common_traits = (
                set(self.trait_names()) &
                set(self.child_spawner.trait_names()) -
                set(self.child_config.keys())
            )
            for trait in common_traits:
                directional_link((self, trait), (self.child_spawner, trait))
        return self.child_spawner

    def load_child_class(self, state):
        # Subclasses must arrange for correct child_class setting from load_state
        pass

    def load_state(self, state):
        super().load_state(state)
        self.load_child_class(state)
        self.child_config.update(state.get('child_conf', {}))
        self.child_state = state.get('child_state', {})
        self.construct_child()

    def get_state(self):
        state = super().get_state()
        state['child_conf'] = self.child_config
        if self.child_spawner:
            self.child_state = state['child_state'] = self.child_spawner.get_state()
        return state

    def clear_state(self):
        super().clear_state()
        if self.child_spawner:
            self.child_spawner.clear_state()
        self.child_state = {}
        self.child_config = {}
        self.child_spawner = None

    # proxy functions for start/poll/stop
    # pass back the child's Future, or create a dummy if needed

    def start(self):
        if not self.child_spawner:
            self.construct_child()
        return self.child_spawner.start()

    def stop(self, now=False):
        if self.child_spawner:
            return self.child_spawner.stop(now)
        else:
            return _yield_val()

    def poll(self):
        if self.child_spawner:
            return self.child_spawner.poll()
        else:
            return _yield_val(1)

    if hasattr(Spawner, 'progress'):
        @property
        def progress(self):
            if self.child_spawner:
                return self.child_spawner.progress
            else:
                raise RuntimeError("No child spawner yet exists - can not get progress yet")


class ProfilesSpawner(WrapSpawner):

    """ProfilesSpawner - leverages the Spawner options form feature to allow user-driven
        configuration of Spawner classes while permitting:
        1) configuration of Spawner classes that don't natively implement options_form
        2) administrator control of allowed configuration changes
        3) runtime choice of which Spawner backend to launch
    """

    profiles = List(
        trait = Tuple( Unicode(), Unicode(), Type(Spawner), Dict() ),
        default_value = [ ( 'Local Notebook Server', 'local', LocalProcessSpawner,
                            {'start_timeout': 15, 'http_timeout': 10} ) ],
        minlen = 1,
        config = True,
        help = """List of profiles to offer for selection. Signature is:
            List(Tuple( Unicode, Unicode, Type(Spawner), Dict )) corresponding to
            profile display name, unique key, Spawner class, dictionary of spawner config options.

            The first three values will be exposed in the input_template as {display}, {key}, and {type}"""
        )

    @validate("profiles")
    def _validate_profiles(self, proposal):
        profiles = proposal.value

        seen = set()
        duplicated = {p[1] for p in profiles if p[1] in seen or seen.add(p[1])}
        if len(duplicated):
            raise TraitError(
                f"Invalid wrapspawner profiles, profiles keys are not unique : {duplicated}")

        return profiles

    child_profile = Unicode()

    form_template = Unicode(
        """<label for="profile">Select a job profile:</label>
        <select class="form-control" name="profile" required autofocus>
        {input_template}
        </select>
        """,
        config = True,
        help = """Template to use to construct options_form text. {input_template} is replaced with
            the result of formatting input_template against each item in the profiles list."""
        )

    first_template = Unicode('selected',
        config=True,
        help="Text to substitute as {first} in input_template"
        )

    input_template = Unicode("""
        <option value="{key}" {first}>{display}</option>""",
        config = True,
        help = """Template to construct {input_template} in form_template. This text will be formatted
            against each item in the profiles list, in order, using the following key names:
            ( display, key, type ) for the first three items in the tuple, and additionally
            first = "checked" (taken from first_template) for the first item in the list, so that
            the first item starts selected."""
        )

    def _options_form_default(self):
        temp_keys = [ dict(display=p[0], key=p[1], type=p[2], first='') for p in self.profiles ]
        temp_keys[0]['first'] = self.first_template
        text = ''.join([ self.input_template.format(**tk) for tk in temp_keys ])
        return self.form_template.format(input_template=text)

    def options_from_form(self, formdata):
        # Default to first profile if somehow none is provided
        return dict(profile=formdata.get('profile', [self.profiles[0][1]])[0])

    # load/get/clear : save/restore child_profile (and on load, use it to update child class/config)

    def select_profile(self, profile):
        # Select matching profile, or do nothing (leaving previous or default config in place)
        for p in self.profiles:
            if p[1] == profile:
                self.child_class = p[2]
                self.child_config = p[3]
                break

    def construct_child(self):
        self.child_profile = self.user_options.get('profile', "")
        self.select_profile(self.child_profile)
        super().construct_child()

    def load_child_class(self, state):
        try:
            self.child_profile = state['profile']
        except KeyError:
            self.child_profile = ''
        self.select_profile(self.child_profile)

    def get_state(self):
        state = super().get_state()
        state['profile'] = self.child_profile
        return state

    def clear_state(self):
        super().clear_state()
        self.child_profile = ''

class DockerProfilesSpawner(ProfilesSpawner):

    """DockerProfilesSpawner - leverages ProfilesSpawner to dynamically create DockerSpawner
        profiles dynamically by looking for docker images that end with "jupyterhub". Due to the
        profiles being dynamic the "profiles" config item from the ProfilesSpawner is renamed as
        "default_profiles". Please note that the "docker" and DockerSpawner packages are required
        for this spawner to work.
    """

    default_profiles = List(
        trait = Tuple( Unicode(), Unicode(), Type(Spawner), Dict() ),
        default_value = [],
        config = True,
        help = """List of profiles to offer in addition to docker images for selection. Signature is:
            List(Tuple( Unicode, Unicode, Type(Spawner), Dict )) corresponding to
            profile display name, unique key, Spawner class, dictionary of spawner config options.

            The first three values will be exposed in the input_template as {display}, {key}, and {type}"""
        )

    docker_spawner_args = Dict(
        default_value = {},
        config = True,
        help = "Args to pass to DockerSpawner."
    )

    jupyterhub_docker_tag_re = re.compile('^.*jupyterhub$')

    def _nvidia_args(self):
        try:
            resp = urllib.request.urlopen('http://localhost:3476/v1.0/docker/cli/json')
            body = resp.read().decode('utf-8')
            args =  json.loads(body)
            return dict(
                read_only_volumes={vol.split(':')[0]: vol.split(':')[1] for vol in args['Volumes']},
                extra_create_kwargs={"volume_driver": args['VolumeDriver']},
                extra_host_config={"devices": args['Devices']},
            )
        except urllib.error.URLError:
            return {}


    def _docker_profile(self, nvidia_args, image):
        spawner_args = dict(container_image=image, network_name=self.user.name)
        spawner_args.update(self.docker_spawner_args)
        spawner_args.update(nvidia_args)
        nvidia_enabled = "w/GPU" if len(nvidia_args) > 0 else "no GPU"
        return ("Docker: (%s): %s"%(nvidia_enabled, image), "docker-%s"%(image), "dockerspawner.SystemUserSpawner", spawner_args)

    def _jupyterhub_docker_tags(self):
        try:
            include_jh_tags = lambda tag: self.jupyterhub_docker_tag_re.match(tag)
            return filter(include_jh_tags, [tag for image in docker.from_env().images.list() for tag in image.tags])
        except NameError:
            raise Exception('The docker package is not installed and is a dependency for DockerProfilesSpawner')

    def _docker_profiles(self):
        return [self._docker_profile(self._nvidia_args(), tag) for tag in self._jupyterhub_docker_tags()]

    @property
    def profiles(self):
        return self.default_profiles + self._docker_profiles()

    @property
    def options_form(self):
        temp_keys = [ dict(display=p[0], key=p[1], type=p[2], first='') for p in self.profiles]
        temp_keys[0]['first'] = self.first_template
        text = ''.join([ self.input_template.format(**tk) for tk in temp_keys ])
        return self.form_template.format(input_template=text)
class DropDownOptionsSpawner(WrapSpawner):

    """DropDownOptionsSpawner - leverages the Spawner options form feature to allow user-driven
        configuration of Spawner classes while permitting:
        1) configuration of Spawner classes that don't natively implement options_form
        2) administrator control of allowed configuration changes
        3) runtime choice of which Spawner backend to launch
    """


    partitions = List(
        trait = Unicode(),
        default_value = ['standard'],
        minlen = 1,
        config = True,
        help = """List of partitions to offer for selection"""
        )

#    default_cluster_idx = Integer(0, config=True)
    default_partition_idx=Integer(0, config=True)
    default_days=Integer(0, config=True)
    default_hours=Integer(8, config=True)
    default_minutes=Integer(0, config=True)
    default_mem=Integer(2, config=True)
    default_cpus=Integer(1, config=True)

    slurmenvironments = List(
        trait = Unicode(),
        default_value=['Default'],
        minlen = 1,
        config=True,
        help="Label for slurm environments"
        )

    options = ""

    form_template = Unicode(
"""
        <label for="partition">Select a partition:</label>
        <select class="form-control" name="partition" required autofocus>
        {partition_template}
        </select>

<table><tr><td width=160px>
        <label for="partition">Number of CPUs:</label>
        <input class="form-control" name="cpus" type="number" min="1" max="64" step="1" value="{default_cpus}" required>
</td><td width=70px/><td width=120px>
        <label for="partition">Memory (GB):</label>
        <input class="form-control" name="memory" type="number" min="1" max="505" step="1" value="{default_mem}" required>
</td><td width=70px/><td width=80px>
        <label for="partition">Days:</label>
        <input class= "form-control" name="days" type="number" min="0" max="5" step="1" value="{default_days}" required>
</td><td width=70px/><td width=80px>
        <label for="partition">Hours:</label>
        <input class="form-control" name="hours" type="number" min="0" max="24" step="1" value="{default_hours}" required>
</td><td width=70px/><td width=80px>
        <label for="partition">Minutes:</label>
        <input class="form-control" name="minutes" type="number" min="0" max="60" step="1" value="{default_minutes}" required>
</td></tr></table>
        <label for="options">Additional options:</label>
        <input class="form-control" name="options" placeholder="--gres=gpu:1 -t 60">
        <label for="cluster">Select Cluster:</label>


        """,
        config = True,
        help = """Template to use to construct options_form text. {input_template} is replaced with
            the result of formatting input_template against each item in the profiles list."""
        )

    first_template = Unicode('selected',
        config=True,
        help="Text to substitute as {first} in input_template"
        )

    partition_template = Unicode("""
        <option value="{key}" {first}>{display}</option>""", config = True)

#    cluster_template = Unicode("""
#        <tr><td width=20px><input class="form-control" name="cluster" type="radio" value="{key}" {first}  </td><td>{display}</td></tr>
#""", config = True)



    day_template = Unicode("""
        <option value="{key}" {first}>{display}</option>""", config = True)

    hour_template = Unicode("""
        <option value="{key}" {first}>{display}</option>""", config = True)

    minute_template = Unicode("""
        <option value="{key}" {first}>{display}</option>""", config = True)


    input_template = Unicode("""
        <option value="{key}" {first}>{display}</option>""",
        config = True,
        help = """Template to construct {input_template} in form_template. This text will be formatted
            against each item in the profiles list, in order, using the following key names:
            ( display, key, type ) for the first three items in the tuple, and additionally
            first = "checked" (taken from first_template) for the first item in the list, so that
            the first item starts selected."""
        )

    options_form = Unicode()

    def _options_form_default(self):

#        import json
#        with open('/home/jcarrol1_guest/profiles.json','r') as outfile:
#            tmp_data=json.load(outfile)
#            for item in tmp_data:
#                self.profiles.append((item['display'],item['key'],SlurmSpawner,item['data']))

#        print(self.profiles)
#                temp_keys.append(dict(display=item['display'],key=item['key'], 
#        with open('data.txt', 'w') as outfile:
#        print(json.dumps([dict(display=p[0],key=p[1],data=p[3]) for p in self.profiles ]))
        

#        temp_keys = [ dict(display=p[0], key=p[1], type=p[2], first='') for p in self.profiles ]
#        temp_keys[0]['first'] = self.first_template
#        text = ''.join([ self.input_template.format(**tk) for tk in temp_keys ])

#        print(self.user)
#        print(self.profiles)

        temp_keys = [ dict(display=p, key=p, first='') for p in self.partitions ]
        temp_keys[self.default_partition_idx]['first'] = self.first_template
        ptext = ''.join([ self.partition_template.format(**tk) for tk in temp_keys ])

        
#        temp_keys = [ dict(display=p, key=p, first='') for p in self.slurmenvironments ]
#        temp_keys[self.default_cluster_idx]['first'] = 'checked'
#        ctext = ''.join([ self.cluster_template.format(**tk) for tk in temp_keys ])

#        temp_keys= [ dict(display=x,key=x, first='') for x in self.days ]
#        temp_keys[self.defaut_day_idx]['first'] = self.first_template
#        dtext = ''.join([ self.day_template.format(**tk) for tk in temp_keys ])

#        temp_keys= [ dict(display=x,key=x, first='') for x in self.hours ]
#        temp_keys[self.default_hour_idx]['first'] = self.first_template
#        htext = ''.join([ self.hour_template.format(**tk) for tk in temp_keys ])

#        temp_keys= [ dict(display=x,key=x, first='') for x in self.minutes ]
#        temp_keys[self.default_minute_idx]['first'] = self.first_template
#        mtext = ''.join([ self.minute_template.format(**tk) for tk in temp_keys ])


#        return self.form_template.format(input_template=text, partition_template=ptext, day_template=dtext, hour_template=htext, minute_template=mtext)
#        return self.form_template.format(input_template=text, partition_template=ptext)
        return self.form_template.format(partition_template=ptext, default_mem=self.default_mem, default_cpus=self.default_cpus, default_days=self.default_days, default_hours=self.default_hours, default_minutes=self.default_minutes)



    def options_from_form(self, formdata):
        # Default to first profile if somehow none is provided
#        temp_dict=profile=formdata.get('profile', [self.profiles[0][1]])[0]       
        return dict(partition=formdata.get('partition',[self.partitions[0][1]])[0],
                    memory=formdata.get('memory',self.default_mem)[0],
                    cpus=formdata.get('cpus',self.default_cpus)[0],
                    days=formdata.get('days',self.default_days)[0],
                    hours=formdata.get('hours',self.default_hours)[0],
                    minutes=formdata.get('minutes',self.default_minutes)[0],
                    options=formdata.get('options','')[0])

#,                   cluster=formdata.get('cluster','')[0])

    # load/get/clear : save/restore child_profile (and on load, use it to update child class/config)

#    def select_profile(self, profile, partition=None, options=None):
        # Select matching profile, or do nothing (leaving previous or default config in place)
#        for p in self.profiles:
#            if p[1] == profile:
#                self.child_class = p[2]
#                self.child_config = p[3]
#                break
#        if partition:
#            print("have partition in select_profile")
#            print(self.partitions)
#            for p in self.partitions:
#                if p[0] == partition:
#                    self.child_config['req_queue'] = p[1]
#                    print(self.child_config)
#        if options:
#            print("found options", options)
#            self.child_config['req_options'] = options
        

    def construct_child(self):
        import inspect
        self.child_class=SlurmSpawner
        self.child_config['req_queue']=self.user_options.get('partition',"")
        self.child_config['req_memory']=self.user_options.get('memory',"")+"GB"
        self.child_config['req_nprocs']=self.user_options.get('cpus',"")
        self.child_config['req_runtime']=self.user_options.get('days',"")+"-"+self.user_options.get('hours',"")+":"+self.user_options.get('minutes',"")+":00"
        self.child_config['req_options']=self.user_options.get('options',"")
#        if self.user_options.get('cluster',"") in self.slurmenvironments:
#            self.child_config['slurmenv']=self.slurmenvironments.index(self.user_options.get('cluster',""))
#        else:
#            self.child_config['slurmenv']=0

#        self.child_profile = self.user_options.get('profile', "")
        
#        partition=self.user_options.get('partition', "")
#        options=self.user_options.get('options',"")
#        print(inspect.getmembers(self))

#        print("self = ", self)
#        print("options = ", self.user_options)
#        print("child_profile = ", self.child_profile)
#        print("partition = ", partition)
#        print("options = ", options)
#        self.select_profile(self.child_profile, partition, options)
#        self.child_class=

        super().construct_child()

#    def load_child_class(self, state):        
#        print(state)
#        self.child_profile = state['profile']
#        self.select_profile(self.child_profile)

    def get_state(self):
        state = super().get_state()
#        state['profile'] = self.child_profile
        return state

    def clear_state(self):
        super().clear_state()
#        self.child_profile = ''


# vim: set ai expandtab softtabstop=4:

