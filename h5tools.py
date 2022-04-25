import os
import h5py


def split(fname_src: str, fname_dest_prefix: str, maxsize_per_file: float):
    """
    Splits an `h5` file into smaller parts, size of each not exceeding
    `maxsize_per_file`.
    """
    idx = 0
    dest_fnames = []
    is_file_open = False
    
    with h5py.File(fname_src, "r") as src:
        for group in src:
            fname = f"{fname_dest_prefix}{idx}.h5"
            
            if not is_file_open:
                dest = h5py.File(fname, "w")
                dest.attrs.update(src.attrs)
                dest_fnames.append(fname)
                is_file_open = True
                
            group_id = dest.require_group(src[group].parent.name)
            src.copy(f"/{group}", group_id, name=group)
            
            if os.path.getsize(fname) > maxsize_per_file:
                dest.close()
                idx += 1
                is_file_open = False            
        dest.close()
        
        return dest_fnames
    

def combine(fname_in: list, fname_out: str):
    """
    Combines a series of `h5` files into a single file.
    """
    with h5py.File(fname_out, "w") as combined:
        for fname in fname_in:
            with h5py.File(fname, "r") as src:
                combined.attrs.update(src.attrs)
                for group in src:
                    group_id = combined.require_group(src[group].parent.name)
                    src.copy(f"/{group}", group_id, name=group)
                    
                    

if __name__ == "__main__":
    prefix = "model_weights_part"
    fname_src = "path_to_large_model_weights_file.h5"
    size_max = 90 * 1024**2  # maximum size allowed in bytes
    fname_parts = split(fname_src, fname_dest_prefix=prefix, maxsize_per_file=size_max)
    combine(fname_in=fname_parts, fname_out="model_weights.h5")