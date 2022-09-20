import gzip

class DashF1DatbaseUtils:

    def compress_file(self, src_file):
        compressed_file = "{}.gz".format(str(src_file))
        with open(src_file, 'rb') as f_in:
            with gzip.open(compressed_file, 'wb') as f_out:
                for line in f_in:
                    f_out.write(line)
        return compressed_file