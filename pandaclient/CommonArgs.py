VALID_TRANSFER_TYPES = {"root", "direct", "davs", "file"}


def add_forceStaged_argument(group):
    group.add_argument(
        "--forceStaged",
        action="store_const",
        const=True,
        dest="forceStaged",
        default=False,
        help="Force files from primary DS to be staged to local disk, even if direct-access is possible",
    )


def add_useDirectIOSites_argument(group):
    group.add_argument(
        "--useDirectIOSites",
        action="store_const",
        const=True,
        dest="useDirectIOSites",
        default=False,
        help="Use only sites which use directIO to read input files",
    )


def add_transfertype_argument(group):
    group.add_argument(
        "--transferType",
        action="store",
        dest="transferType",
        default=None,
        metavar="TYPE[,TYPE...]",
        help="Comma-separated transfer types to restrict input access. Allowed values: root, direct, davs, file",
    )


def get_invalid_transfer_types(transfer_type_str):
    if transfer_type_str is None:
        return set()
    return set(transfer_type_str.split(",")) - VALID_TRANSFER_TYPES


def add_common_arguments(group_submit, group_input):
    add_useDirectIOSites_argument(group_submit)
    add_transfertype_argument(group_submit)
    add_forceStaged_argument(group_input)
